//! RPC access.
//!
//! Speaks gRPC to the node directly through `miden-node-proto`. No client, no
//! store, no keystore.

use std::time::Duration;

use anyhow::{Context, Result};
use miden_node_proto::DecodeMessageExt;
use miden_node_proto::clients::{Builder, RpcClient};
use miden_node_proto::domain::account::GetAccountResponse as VerifiedAccount;
use miden_node_proto::generated::miden::node::v1::{
    GetAccountRequest, GetAccountResponse, get_account_request,
};
use miden_protocol::EMPTY_WORD;
use miden_protocol::account::AccountId;
use url::Url;

/// Connects an RPC client to `rpc_url` over TLS.
pub async fn connect(rpc_url: &str, timeout: Duration) -> Result<RpcClient> {
    let url = Url::parse(rpc_url).context("invalid RPC URL")?;
    Builder::new(url)
        .with_tls()
        .context("TLS setup")?
        .with_timeout(timeout)
        .without_metadata_version()
        .without_metadata_genesis()
        .without_auth_header()
        .without_otel_context_injection()
        .connect::<RpcClient>()
        .await
        .context("connect to RPC")
}

/// The chain tip, used to start the block subscription at the present rather
/// than replaying every block since genesis.
pub async fn chain_tip(client: &mut RpcClient) -> Result<u32> {
    let status = client
        .status(miden_node_proto::generated::miden::node::v1::StatusRequest {})
        .await
        .context("status RPC")?
        .into_inner();
    Ok(status.chain_tip)
}

/// Fetches a public account with its value slots and vault.
///
/// `asset_vault_commitment` must be present for the response to carry vault
/// assets at all: an absent field means "no vault data", and the empty word
/// (which no real vault root equals) means "always include it". Sending `None`
/// here reports every pool as holding zero reserves.
///
/// Storage map details are not requested, so the response carries the storage
/// header (value slots) and the vault assets only. Registering accounts use a
/// separate call with `all_storage_maps`.
pub async fn fetch_account(
    client: &mut RpcClient,
    account_id: AccountId,
) -> Result<VerifiedAccount> {
    let request = GetAccountRequest {
        account_id: Some((&account_id).into()),
        block_num: None,
        details: Some(get_account_request::AccountDetailRequest {
            code_commitment: None,
            asset_vault_commitment: Some(EMPTY_WORD.into()),
            storage_request: None,
        }),
    };

    let raw: GetAccountResponse = client
        .get_account(request)
        .await
        .context("get_account RPC")?
        .into_inner();

    raw.decode_and_verify()
        .context("decode get_account response")
}

/// Fetches a public account with every storage map entry.
pub async fn fetch_account_with_maps(
    client: &mut RpcClient,
    account_id: AccountId,
) -> Result<VerifiedAccount> {
    let request = GetAccountRequest {
        account_id: Some((&account_id).into()),
        block_num: None,
        details: Some(get_account_request::AccountDetailRequest {
            code_commitment: None,
            asset_vault_commitment: Some(EMPTY_WORD.into()),
            storage_request: Some(
                get_account_request::account_detail_request::StorageRequest::AllStorageMaps(true),
            ),
        }),
    };

    let raw: GetAccountResponse = client
        .get_account(request)
        .await
        .context("get_account RPC")?
        .into_inner();

    raw.decode_and_verify()
        .context("decode get_account response")
}
