import asyncio
import time

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from loguru import logger

from ..core.errors import KeysetNotFoundError
from ..core.models import (
    GetInfoResponse,
    KeysetsResponse,
    KeysetsResponseKeyset,
    KeysResponse,
    KeysResponseKeyset,
    PostCheckStateRequest,
    PostCheckStateResponse,
    PostMeltQuoteRequest,
    PostMeltQuoteResponse,
    PostMeltRequest,
    PostMintQuoteRequest,
    PostMintQuoteResponse,
    PostMintRequest,
    PostMintResponse,
    PostRestoreRequest,
    PostRestoreResponse,
    PostSwapRequest,
    PostSwapResponse,
)
from ..core.settings import settings
from ..mint.startup import ledger
from .cache import RedisCache
from .limit import limit_websocket, limiter
from .opentimestamps import decode_ots_proof

router = APIRouter()
redis = RedisCache()


@router.get(
    "/v1/info",
    name="Mint information",
    summary="Mint information, operator contact information, and other info.",
    response_model=GetInfoResponse,
    response_model_exclude_none=True,
)
async def info() -> GetInfoResponse:
    logger.trace("> GET /v1/info")
    mint_info = ledger.mint_info
    return GetInfoResponse(
        name=mint_info.name,
        pubkey=mint_info.pubkey,
        version=mint_info.version,
        description=mint_info.description,
        description_long=mint_info.description_long,
        contact=mint_info.contact,
        nuts=mint_info.nuts,
        icon_url=mint_info.icon_url,
        tos_url=mint_info.tos_url,
        urls=settings.mint_info_urls,
        motd=mint_info.motd,
        time=int(time.time()),
    )


@router.get(
    "/v1/keys",
    name="Mint public keys",
    summary="Get the public keys of the newest mint keyset",
    response_description=(
        "All supported token values their associated public keys for all active keysets"
    ),
    response_model=KeysResponse,
)
async def keys():
    """This endpoint returns a dictionary of all supported token values of the mint and their associated public key."""
    logger.trace("> GET /v1/keys")
    keyset = ledger.keyset
    keyset_for_response = []
    for keyset in ledger.keysets.values():
        if keyset.active:
            keyset_for_response.append(
                KeysResponseKeyset(
                    id=keyset.id,
                    unit=keyset.unit.name,
                    active=keyset.active,
                    input_fee_ppk=keyset.input_fee_ppk,
                    keys={k: v for k, v in keyset.public_keys_hex.items()},
                )
            )
    return KeysResponse(keysets=keyset_for_response)


@router.get(
    "/v1/keys/{keyset_id}",
    name="Keyset public keys",
    summary="Public keys of a specific keyset",
    response_description=(
        "All supported token values of the mint and their associated"
        " public key for a specific keyset."
    ),
    response_model=KeysResponse,
)
async def keyset_keys(keyset_id: str) -> KeysResponse:
    """
    Get the public keys of the mint from a specific keyset id.
    """
    logger.trace(f"> GET /v1/keys/{keyset_id}")
    # BEGIN BACKWARDS COMPATIBILITY < 0.15.0
    # if keyset_id is not hex, we assume it is base64 and sanitize it
    try:
        int(keyset_id, 16)
    except ValueError:
        keyset_id = keyset_id.replace("-", "+").replace("_", "/")
    # END BACKWARDS COMPATIBILITY < 0.15.0

    keyset = ledger.keysets.get(keyset_id)
    if keyset is None:
        raise KeysetNotFoundError(keyset_id)

    keyset_for_response = KeysResponseKeyset(
        id=keyset.id,
        unit=keyset.unit.name,
        active=keyset.active,
        input_fee_ppk=keyset.input_fee_ppk,
        keys={k: v for k, v in keyset.public_keys_hex.items()},
    )
    return KeysResponse(keysets=[keyset_for_response])


@router.get(
    "/v1/keysets",
    name="Active keysets",
    summary="Get all active keyset id of the mind",
    response_model=KeysetsResponse,
    response_description="A list of all active keyset ids of the mint.",
)
async def keysets() -> KeysetsResponse:
    """This endpoint returns a list of keysets that the mint currently supports and will accept tokens from."""
    logger.trace("> GET /v1/keysets")
    keysets = []
    for id, keyset in ledger.keysets.items():
        keysets.append(
            KeysetsResponseKeyset(
                id=keyset.id,
                unit=keyset.unit.name,
                active=keyset.active,
                input_fee_ppk=keyset.input_fee_ppk,
            )
        )
    return KeysetsResponse(keysets=keysets)


@router.post(
    "/v1/mint/quote/bolt11",
    name="Request mint quote",
    summary="Request a quote for minting of new tokens",
    response_model=PostMintQuoteResponse,
    response_description="A payment request to mint tokens of a denomination",
)
@limiter.limit(f"{settings.mint_transaction_rate_limit_per_minute}/minute")
async def mint_quote(
    request: Request, payload: PostMintQuoteRequest
) -> PostMintQuoteResponse:
    """
    Request minting of new tokens. The mint responds with a Lightning invoice.
    This endpoint can be used for a Lightning invoice UX flow.

    Call `POST /v1/mint/bolt11` after paying the invoice.
    """
    logger.trace(f"> POST /v1/mint/quote/bolt11: payload={payload}")
    quote = await ledger.mint_quote(payload)
    resp = PostMintQuoteResponse(
        quote=quote.quote,
        request=quote.request,
        amount=quote.amount,
        unit=quote.unit,
        paid=quote.paid,  # deprecated
        state=quote.state.value,
        expiry=quote.expiry,
        pubkey=quote.pubkey,
    )
    logger.trace(f"< POST /v1/mint/quote/bolt11: {resp}")
    return resp


@router.get(
    "/v1/mint/quote/bolt11/{quote}",
    summary="Get mint quote",
    response_model=PostMintQuoteResponse,
    response_description="Get an existing mint quote to check its status.",
)
@limiter.limit(f"{settings.mint_transaction_rate_limit_per_minute}/minute")
async def get_mint_quote(request: Request, quote: str) -> PostMintQuoteResponse:
    """
    Get mint quote state.
    """
    logger.trace(f"> GET /v1/mint/quote/bolt11/{quote}")
    mint_quote = await ledger.get_mint_quote(quote)
    resp = PostMintQuoteResponse(
        quote=mint_quote.quote,
        request=mint_quote.request,
        amount=mint_quote.amount,
        unit=mint_quote.unit,
        paid=mint_quote.paid,  # deprecated
        state=mint_quote.state.value,
        expiry=mint_quote.expiry,
        pubkey=mint_quote.pubkey,
    )
    logger.trace(f"< GET /v1/mint/quote/bolt11/{quote}")
    return resp


@router.websocket("/v1/ws", name="Websocket endpoint for subscriptions")
async def websocket_endpoint(websocket: WebSocket):
    limit_websocket(websocket)
    disconnected = False
    try:
        client = ledger.events.add_client(websocket, ledger.db, ledger.crud)
    except Exception as e:
        logger.debug(f"Exception: {e}")
        await asyncio.wait_for(websocket.close(), timeout=1)
        return

    try:
        # this will block until the session is closed
        await client.start()
    except WebSocketDisconnect as e:
        logger.debug(f"Websocket disconnected: {e}")
        disconnected = True
        return
    except Exception as e:
        logger.debug(f"Exception: {e}")
        ledger.events.remove_client(client)
    finally:
        if not disconnected:
            await asyncio.wait_for(websocket.close(), timeout=1)


@router.post(
    "/v1/mint/bolt11",
    name="Mint tokens with a Lightning payment",
    summary="Mint tokens by paying a bolt11 Lightning invoice.",
    response_model=PostMintResponse,
    response_description=(
        "A list of blinded signatures that can be used to create proofs."
    ),
)
@limiter.limit(f"{settings.mint_transaction_rate_limit_per_minute}/minute")
@redis.cache()
async def mint(
    request: Request,
    payload: PostMintRequest,
) -> PostMintResponse:
    """
    Requests the minting of tokens belonging to a paid payment request.

    Call this endpoint after `POST /v1/mint/quote`.
    """
    logger.trace(f"> POST /v1/mint/bolt11: {payload}")

    promises = await ledger.mint(
        outputs=payload.outputs, quote_id=payload.quote, signature=payload.signature
    )
    blinded_signatures = PostMintResponse(signatures=promises)
    logger.trace(f"< POST /v1/mint/bolt11: {blinded_signatures}")
    return blinded_signatures


@router.post(
    "/v1/melt/quote/bolt11",
    summary="Request a quote for melting tokens",
    response_model=PostMeltQuoteResponse,
    response_description="Melt tokens for a payment on a supported payment method.",
)
@limiter.limit(f"{settings.mint_transaction_rate_limit_per_minute}/minute")
async def melt_quote(
    request: Request, payload: PostMeltQuoteRequest
) -> PostMeltQuoteResponse:
    """
    Request a quote for melting tokens.
    """
    logger.trace(f"> POST /v1/melt/quote/bolt11: {payload}")
    quote = await ledger.melt_quote(payload)  # TODO
    logger.trace(f"< POST /v1/melt/quote/bolt11: {quote}")
    return quote


@router.get(
    "/v1/melt/quote/bolt11/{quote}",
    summary="Get melt quote",
    response_model=PostMeltQuoteResponse,
    response_description="Get an existing melt quote to check its status.",
)
@limiter.limit(f"{settings.mint_transaction_rate_limit_per_minute}/minute")
async def get_melt_quote(request: Request, quote: str) -> PostMeltQuoteResponse:
    """
    Get melt quote state.
    """
    logger.trace(f"> GET /v1/melt/quote/bolt11/{quote}")
    melt_quote = await ledger.get_melt_quote(quote)
    resp = PostMeltQuoteResponse(
        quote=melt_quote.quote,
        amount=melt_quote.amount,
        unit=melt_quote.unit,
        request=melt_quote.request,
        fee_reserve=melt_quote.fee_reserve,
        paid=melt_quote.paid,
        state=melt_quote.state.value,
        expiry=melt_quote.expiry,
        payment_preimage=melt_quote.payment_preimage,
        change=melt_quote.change,
    )
    logger.trace(f"< GET /v1/melt/quote/bolt11/{quote}")
    return resp


@router.post(
    "/v1/melt/bolt11",
    name="Melt tokens",
    summary=(
        "Melt tokens for a Bitcoin payment that the mint will make for the user in"
        " exchange"
    ),
    response_model=PostMeltQuoteResponse,
    response_description=(
        "The state of the payment, a preimage as proof of payment, and a list of"
        " promises for change."
    ),
)
@limiter.limit(f"{settings.mint_transaction_rate_limit_per_minute}/minute")
@redis.cache()
async def melt(request: Request, payload: PostMeltRequest) -> PostMeltQuoteResponse:
    """
    Requests tokens to be destroyed and sent out via Lightning.
    """
    logger.trace(f"> POST /v1/melt/bolt11: {payload}")
    resp = await ledger.melt(
        proofs=payload.inputs, quote=payload.quote, outputs=payload.outputs
    )
    logger.trace(f"< POST /v1/melt/bolt11: {resp}")
    return resp


@router.post(
    "/v1/swap",
    name="Swap tokens",
    summary="Swap inputs for outputs of the same value",
    response_model=PostSwapResponse,
    response_description=(
        "An array of blinded signatures that can be used to create proofs."
    ),
)
@limiter.limit(f"{settings.mint_transaction_rate_limit_per_minute}/minute")
@redis.cache()
async def swap(
    request: Request,
    payload: PostSwapRequest,
) -> PostSwapResponse:
    """
    Requests a set of Proofs to be swapped for another set of BlindSignatures.

    This endpoint can be used by Alice to swap a set of proofs before making a payment to Carol.
    It can then used by Carol to redeem the tokens for new proofs.
    """
    logger.trace(f"> POST /v1/swap: {payload}")
    assert payload.outputs, Exception("no outputs provided.")

    signatures = await ledger.swap(proofs=payload.inputs, outputs=payload.outputs)

    return PostSwapResponse(signatures=signatures)


@router.post(
    "/v1/checkstate",
    name="Check proof state",
    summary="Check whether a proof is spent already or is pending in a transaction",
    response_model=PostCheckStateResponse,
    response_description=(
        "Two lists of booleans indicating whether the provided proofs "
        "are spendable or pending in a transaction respectively."
    ),
)
async def check_state(
    payload: PostCheckStateRequest,
) -> PostCheckStateResponse:
    """Check whether a secret has been spent already or not."""
    logger.trace(f"> POST /v1/checkstate: {payload}")
    proof_states = await ledger.db_read.get_proofs_states(payload.Ys)
    return PostCheckStateResponse(states=proof_states)


@router.post(
    "/v1/restore",
    name="Restore",
    summary="Restores blind signature for a set of outputs.",
    response_model=PostRestoreResponse,
    response_description=(
        "Two lists with the first being the list of the provided outputs that "
        "have an associated blinded signature which is given in the second list."
    ),
)
async def restore(payload: PostRestoreRequest) -> PostRestoreResponse:
    assert payload.outputs, Exception("no outputs provided.")
    outputs, signatures = await ledger.restore(payload.outputs)
    return PostRestoreResponse(outputs=outputs, signatures=signatures)


_pol_service_instance = None


def _get_pol_service():
    global _pol_service_instance
    if _pol_service_instance is None:
        from .pol_service import PoLService
        _pol_service_instance = PoLService(
            db=ledger.db,
            crud=ledger.crud,
            keysets=ledger.keysets,
            mint_pubkey=ledger.pubkey.format().hex(),
            seed=ledger.seed,
        )
    return _pol_service_instance


@router.get(
    "/v1/pol/roots/{keyset_id}",
    name="Get PoL Merkle roots",
    summary="Get current Merkle Sum Tree roots",
    tags=["Proof of Liabilities"],
)
async def get_pol_roots(keyset_id: str, epoch_date: str = None):
    """Get current Merkle Sum Tree roots for a keyset."""
    service = _get_pol_service()
    roots = await service.get_merkle_roots(keyset_id, epoch_date)
    return roots.model_dump()


@router.get(
    "/v1/pol/history/{keyset_id}",
    name="Get PoL epoch history",
    summary="Get historical epochs",
    tags=["Proof of Liabilities"],
)
async def get_pol_history(keyset_id: str, limit: int = 30):
    """Get historical closed epochs for a keyset."""
    service = _get_pol_service()
    history = await service.get_epoch_history(keyset_id, limit)
    return history.model_dump()


@router.get(
    "/v1/pol/verify/mint/{keyset_id}/{B_}",
    name="Verify mint inclusion",
    summary="Check if B_ is in mint tree",
    tags=["Proof of Liabilities"],
)
async def verify_mint_proof(keyset_id: str, B_: str, epoch_date: str = None):
    """Verify a B_ is included in the mint Merkle tree."""
    service = _get_pol_service()
    status, proof = await service.get_mint_proof_inclusion(keyset_id, B_, epoch_date)
    return {
        "keyset_id": keyset_id,
        "B_": B_,
        "status": status,
        "proof": proof.model_dump() if proof else None,
    }


@router.get(
    "/v1/pol/verify/burn/{keyset_id}/{Y}",
    name="Verify burn inclusion",
    summary="Check if Y is in burn tree",
    tags=["Proof of Liabilities"],
)
async def verify_burn_proof(keyset_id: str, Y: str, epoch_date: str = None):
    """Verify a Y is included in the burn Merkle tree."""
    service = _get_pol_service()
    status, proof = await service.get_burn_proof_inclusion(keyset_id, Y, epoch_date)
    return {
        "keyset_id": keyset_id,
        "Y": Y,
        "status": status,
        "proof": proof.model_dump() if proof else None,
    }


@router.get(
    "/v1/pol/commitment/{keyset_id}/{B_}",
    name="Get mint commitment",
    summary="Get signed commitment for pending token",
    tags=["Proof of Liabilities"],
)
async def get_mint_commitment(keyset_id: str, B_: str, amount: int):
    """
    Get a signed commitment promising token inclusion in today's epoch.
    
    The wallet can use this commitment as proof of mint misbehavior if the
    token is not included in tomorrow's closed Merkle tree.
    
    Only available for tokens minted in the current (open) epoch.
    """
    service = _get_pol_service()
    
    # Verify this token exists in today's data
    status, _ = await service.get_mint_proof_inclusion(keyset_id, B_)
    
    if status == "NOT_FOUND":
        raise HTTPException(
            status_code=404,
            detail="Token not found in current epoch"
        )
    
    if status == "INCLUDED":
        # Token is in a closed epoch, no commitment needed
        return {
            "keyset_id": keyset_id,
            "B_": B_,
            "status": "ALREADY_CLOSED",
            "commitment": None,
            "message": "Token is already in a closed epoch with verifiable proof"
        }
    
    # status == "PENDING_EPOCH" - create commitment
    commitment = service.create_mint_commitment(keyset_id, B_, amount)
    return {
        "keyset_id": keyset_id,
        "B_": B_,
        "status": "PENDING_EPOCH",
        "commitment": commitment.model_dump(),
    }


@router.get(
    "/v1/pol/ots/{keyset_id}/{epoch_date}",
    name="Download OTS file",
    summary="Download raw .ots file",
    tags=["Proof of Liabilities"],
)
async def download_ots_file(keyset_id: str, epoch_date: str):
    """Download the raw binary .ots file for a specific keyset epoch."""
    service = _get_pol_service()
    
    reports = await service.crud.get_pol_reports_by_date(
        epoch_date=epoch_date, db=service.db
    )

    target_report = next((r for r in reports if r.get("keyset_id") == keyset_id), None)

    if not target_report:
        raise HTTPException(
            status_code=404, 
            detail=f"No reports found for keyset {keyset_id} on {epoch_date}"
        )

    ots_proof_b64 = target_report.get("ots_proof")

    if not ots_proof_b64:
        raise HTTPException(
            status_code=404, 
            detail="No OTS proof available yet for this epoch"
        )

    ots_bytes = decode_ots_proof(ots_proof_b64)
    if ots_bytes is None:
        raise HTTPException(status_code=500, detail="Failed to decode OTS proof")

    short_id = keyset_id[:12]
    filename = f"pol_{short_id}_{epoch_date}.ots"

    return Response(
        content=ots_bytes,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Report-Hash": target_report.get("report_hash", ""),
            "X-OTS-Confirmed": "true" if target_report.get("ots_confirmed") else "false",
        }
    )