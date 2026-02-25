import asyncio
import hashlib
import json
import time
from typing import Dict, List, Optional, Tuple

from loguru import logger

from ..core.crypto.secp import PrivateKey
from ..core.db import Database
from ..core.proof_of_liabilities import (
    BurnProof,
    ChainValidationResult,
    EpochReport,
    MerkleSumProof,
    MerkleSumTree,
    MintCommitment,
    MintProof,
    PoLMerkleRoots,
    PoLHistory,
    get_epoch_date,
    get_epoch_boundaries,
    calculate_expires_at,
)
from ..core.settings import settings
from .crud import LedgerCrud
from .opentimestamps import (
    OTSStatus,
    submit_to_opentimestamps,
    upgrade_opentimestamps_proof,
)


def _parse_timestamp(value) -> int:
    if value is None:
        return 0
    if hasattr(value, "timestamp"):
        return int(value.timestamp())
    return value if isinstance(value, int) else 0


class PoLService:
    def __init__(
        self,
        db: Database,
        crud: LedgerCrud,
        keysets: Dict,
        mint_pubkey: str,
        seed: str,
    ):
        self.db = db
        self.crud = crud
        self.keysets = keysets
        self.mint_pubkey = mint_pubkey
        self.private_key = PrivateKey(
            hashlib.sha256(seed.encode("utf-8")).digest()[:32]
        )

        self._mint_trees: Dict[Tuple[str, str], MerkleSumTree] = {}
        self._burn_trees: Dict[Tuple[str, str], MerkleSumTree] = {}
        self._mint_data: Dict[Tuple[str, str], List[MintProof]] = {}
        self._burn_data: Dict[Tuple[str, str], List[BurnProof]] = {}

    async def _load_proofs_for_epoch(
        self, keyset_id: str, epoch_date: str
    ) -> Tuple[List[MintProof], List[BurnProof]]:
        epoch_start, epoch_end = get_epoch_boundaries(epoch_date)
        
        mint_proofs_raw = await self.crud.get_mint_proofs_in_range(
            keyset_id=keyset_id,
            start_time=epoch_start,
            end_time=epoch_end,
            db=self.db,
        )
        burn_proofs_raw = await self.crud.get_burn_proofs_in_range(
            keyset_id=keyset_id,
            start_time=epoch_start,
            end_time=epoch_end,
            db=self.db,
        )
        
        mint_proofs = [
            MintProof(
                keyset_id=row["keyset_id"],
                amount=row["amount"],
                B_=row["b_"],
                C_=row["c_"],
                dleq_e=row.get("dleq_e"),
                dleq_s=row.get("dleq_s"),
                created=_parse_timestamp(row.get("created")),
            )
            for row in mint_proofs_raw
        ]
        
        burn_proofs = [
            BurnProof(
                keyset_id=row["keyset_id"],
                amount=row["amount"],
                secret=row["secret"],
                Y=row["y"],
                C=row["c"],
                witness=row.get("witness"),
                created=_parse_timestamp(row.get("created")),
            )
            for row in burn_proofs_raw
        ]
        
        return mint_proofs, burn_proofs

    async def _build_merkle_trees(
        self,
        keyset_id: str,
        epoch_date: str,
        mint_proofs: List[MintProof],
        burn_proofs: List[BurnProof],
        use_fixed_padding: bool = False,
    ) -> Tuple[MerkleSumTree, MerkleSumTree]:
        cache_key = (keyset_id, epoch_date)
        
        mint_leaves = [(p.B_, p.amount) for p in mint_proofs]
        burn_leaves = [(p.Y, p.amount) for p in burn_proofs]
        
        fixed_tree_size = 2 ** settings.pol_tree_size_exponent if use_fixed_padding else None
        
        loop = asyncio.get_event_loop()
        mint_tree, burn_tree = await loop.run_in_executor(
            None,
            lambda: (
                MerkleSumTree(
                    mint_leaves,
                    use_fixed_padding=use_fixed_padding,
                    epoch_date=epoch_date,
                    fixed_tree_size=fixed_tree_size,
                ),
                MerkleSumTree(
                    burn_leaves,
                    use_fixed_padding=use_fixed_padding,
                    epoch_date=epoch_date,
                    fixed_tree_size=fixed_tree_size,
                ),
            ),
        )
        
        self._mint_trees[cache_key] = mint_tree
        self._burn_trees[cache_key] = burn_tree
        self._mint_data[cache_key] = mint_proofs
        self._burn_data[cache_key] = burn_proofs
        
        return mint_tree, burn_tree

    async def _get_previous_epoch(
        self, keyset_id: str, epoch_date: str
    ) -> Optional[EpochReport]:
        stored = await self.crud.get_pol_reports_for_keyset(
            keyset_id=keyset_id, db=self.db, limit=100
        )
        
        sorted_rows = sorted(stored, key=lambda r: r["epoch_date"], reverse=True)
        
        for row in sorted_rows:
            if row["epoch_date"] < epoch_date:
                return EpochReport(
                    keyset_id=row["keyset_id"],
                    epoch_date=row["epoch_date"],
                    epoch_start=row["epoch_start"],
                    epoch_end=row["epoch_end"],
                    previous_epoch_hash=row.get("previous_epoch_hash"),
                    cumulative_minted=row.get("cumulative_minted", 0),
                    cumulative_burned=row.get("cumulative_burned", 0),
                    total_minted=row["total_minted"],
                    total_burned=row["total_burned"],
                    outstanding_balance=row["outstanding_balance"],
                    mint_root_hash=row["mint_root_hash"],
                    mint_root_amount=row["mint_root_amount"],
                    burn_root_hash=row["burn_root_hash"],
                    burn_root_amount=row["burn_root_amount"],
                    report_timestamp=row["report_timestamp"],
                    report_hash=row["report_hash"],
                    report_signature=row.get("report_signature") or "",
                    ots_proof=row.get("ots_proof"),
                    ots_confirmed=bool(row.get("ots_confirmed", False)),
                )
        
        return None

    async def generate_epoch_report(
        self,
        keyset_id: str,
        epoch_date: str,
        include_chain_link: bool = True,
        use_fixed_padding: bool = False,
    ) -> EpochReport:
        if keyset_id not in self.keysets:
            raise ValueError(f"Keyset {keyset_id} not found")
        
        epoch_start, epoch_end = get_epoch_boundaries(epoch_date)
        mint_proofs, burn_proofs = await self._load_proofs_for_epoch(
            keyset_id, epoch_date
        )
        
        mint_tree, burn_tree = await self._build_merkle_trees(
            keyset_id, epoch_date, mint_proofs, burn_proofs,
            use_fixed_padding=use_fixed_padding,
        )
        
        total_minted = sum(p.amount for p in mint_proofs)
        total_burned = sum(p.amount for p in burn_proofs)
        
        previous_epoch_hash = None
        cumulative_minted = total_minted
        cumulative_burned = total_burned
        
        if include_chain_link:
            previous = await self._get_previous_epoch(keyset_id, epoch_date)
            if previous:
                previous_epoch_hash = previous.report_hash
                cumulative_minted = previous.cumulative_minted + total_minted
                cumulative_burned = previous.cumulative_burned + total_burned
        
        report_data = {
            "keyset_id": keyset_id,
            "epoch_date": epoch_date,
            "epoch_start": epoch_start,
            "epoch_end": epoch_end,
            "previous_epoch_hash": previous_epoch_hash,
            "cumulative_minted": cumulative_minted,
            "cumulative_burned": cumulative_burned,
            "total_minted": total_minted,
            "total_burned": total_burned,
            "outstanding_balance": cumulative_minted - cumulative_burned,
            "mint_root_hash": mint_tree.root_hash,
            "mint_root_amount": mint_tree.total_amount,
            "burn_root_hash": burn_tree.root_hash,
            "burn_root_amount": burn_tree.total_amount,
        }
        report_hash = hashlib.sha256(
            json.dumps(report_data, sort_keys=True, separators=(',', ':')).encode()
        ).hexdigest()
        
        report_hash_bytes = bytes.fromhex(report_hash)
        signature = self.private_key.sign_schnorr(report_hash_bytes, None)
        
        expires_at = calculate_expires_at(
            epoch_date, settings.pol_retention_months
        )
        
        report = EpochReport(
            keyset_id=keyset_id,
            epoch_date=epoch_date,
            epoch_start=epoch_start,
            epoch_end=epoch_end,
            previous_epoch_hash=previous_epoch_hash,
            cumulative_minted=cumulative_minted,
            cumulative_burned=cumulative_burned,
            total_minted=total_minted,
            total_burned=total_burned,
            outstanding_balance=cumulative_minted - cumulative_burned,
            mint_root_hash=mint_tree.root_hash,
            mint_root_amount=mint_tree.total_amount,
            burn_root_hash=burn_tree.root_hash,
            burn_root_amount=burn_tree.total_amount,
            report_timestamp=int(time.time()),
            report_hash=report_hash,
            report_signature=signature.hex(),
            expires_at=expires_at,
        )
        
        return report
    
    async def get_merkle_roots(
        self, keyset_id: str, epoch_date: Optional[str] = None
    ) -> PoLMerkleRoots:
        """Get Merkle roots for a keyset epoch."""
        if epoch_date is None:
            epoch_date = get_epoch_date()
        
        today = get_epoch_date()
        is_closed = epoch_date < today
        
        stored = await self.crud.get_pol_report_for_epoch(
            keyset_id=keyset_id, epoch_date=epoch_date, db=self.db
        )
        
        if stored:
            return PoLMerkleRoots(
                keyset_id=keyset_id,
                epoch_date=epoch_date,
                is_closed=is_closed,
                previous_epoch_hash=stored.get("previous_epoch_hash"),
                mint_root_hash=stored["mint_root_hash"],
                mint_root_amount=stored["mint_root_amount"],
                burn_root_hash=stored["burn_root_hash"],
                burn_root_amount=stored["burn_root_amount"],
                outstanding_balance=stored["outstanding_balance"],
                cumulative_minted=stored.get("cumulative_minted", 0),
                cumulative_burned=stored.get("cumulative_burned", 0),
                report_timestamp=stored["report_timestamp"],
                report_hash=stored["report_hash"],
                report_signature=stored.get("report_signature") or "",
                ots_proof=stored.get("ots_proof"),
                ots_confirmed=bool(stored.get("ots_confirmed", False)),
            )
        
        report = await self.generate_epoch_report(keyset_id, epoch_date)
        
        return PoLMerkleRoots(
            keyset_id=keyset_id,
            epoch_date=epoch_date,
            is_closed=is_closed,
            previous_epoch_hash=report.previous_epoch_hash,
            mint_root_hash=report.mint_root_hash,
            mint_root_amount=report.mint_root_amount,
            burn_root_hash=report.burn_root_hash,
            burn_root_amount=report.burn_root_amount,
            outstanding_balance=report.outstanding_balance,
            cumulative_minted=report.cumulative_minted,
            cumulative_burned=report.cumulative_burned,
            report_timestamp=report.report_timestamp,
            report_hash=report.report_hash if is_closed else None,
            report_signature=report.report_signature if is_closed else None,
        )
    
    async def get_mint_proof_inclusion(
        self, keyset_id: str, B_: str, epoch_date: Optional[str] = None
    ) -> Tuple[str, Optional[MerkleSumProof]]:
        """Get Merkle Sum proof for a mint proof (B_).
        
        Returns:
            Tuple of (status, proof) where status is:
            - "INCLUDED": Token found in closed epoch
            - "PENDING_EPOCH": Token found in open (current) epoch
            - "NOT_FOUND": Token not found
        """
        if epoch_date is None:
            epoch_date = get_epoch_date()
        
        today = get_epoch_date()
        is_open_epoch = epoch_date >= today
        
        cache_key = (keyset_id, epoch_date)
        
        if cache_key not in self._mint_trees:
            await self.generate_epoch_report(keyset_id, epoch_date)
        
        mint_tree = self._mint_trees.get(cache_key)
        if not mint_tree:
            return ("NOT_FOUND", None)
        
        proof = mint_tree.get_proof(B_)
        if proof is None:
            return ("NOT_FOUND", None)
        
        if is_open_epoch:
            # Open epoch: token seen but no proof available yet
            return ("PENDING_EPOCH", None)
        
        return ("INCLUDED", proof)
    
    async def get_burn_proof_inclusion(
        self, keyset_id: str, Y: str, epoch_date: Optional[str] = None
    ) -> Tuple[str, Optional[MerkleSumProof]]:
        """Get Merkle Sum proof for a burn proof (Y).
        
        Returns:
            Tuple of (status, proof) where status is:
            - "INCLUDED": Token found in closed epoch
            - "PENDING_EPOCH": Token found in open (current) epoch
            - "NOT_FOUND": Token not found
        """
        if epoch_date is None:
            epoch_date = get_epoch_date()
        
        today = get_epoch_date()
        is_open_epoch = epoch_date >= today
        
        cache_key = (keyset_id, epoch_date)
        
        if cache_key not in self._burn_trees:
            await self.generate_epoch_report(keyset_id, epoch_date)
        
        burn_tree = self._burn_trees.get(cache_key)
        if not burn_tree:
            return ("NOT_FOUND", None)
        
        proof = burn_tree.get_proof(Y)
        if proof is None:
            return ("NOT_FOUND", None)
        
        if is_open_epoch:
            return ("PENDING_EPOCH", None)
        
        return ("INCLUDED", proof)
    
    async def verify_mint_inclusion(
        self, keyset_id: str, B_: str, epoch_date: Optional[str] = None
    ) -> bool:
        """Verify a B_ is included in the mint Merkle tree."""
        status, proof = await self.get_mint_proof_inclusion(keyset_id, B_, epoch_date)
        return status == "INCLUDED" and proof is not None and proof.verify()
    
    async def verify_burn_inclusion(
        self, keyset_id: str, Y: str, epoch_date: Optional[str] = None
    ) -> bool:
        """Verify a Y is included in the burn Merkle tree."""
        status, proof = await self.get_burn_proof_inclusion(keyset_id, Y, epoch_date)
        return status == "INCLUDED" and proof is not None and proof.verify()
    
    async def get_epoch_report(
        self, keyset_id: str, epoch_date: str
    ) -> Optional[dict]:
        """Get a specific epoch report."""
        report = await self.crud.get_pol_report_for_epoch(
            keyset_id=keyset_id, epoch_date=epoch_date, db=self.db
        )
        return report
    
    async def get_epoch_history(
        self, keyset_id: str, limit: int = 30
    ) -> PoLHistory:
        """Get historical closed epochs for a keyset."""
        stored = await self.crud.get_pol_reports_for_keyset(
            keyset_id=keyset_id, db=self.db, limit=limit
        )
        
        epochs = [
            EpochReport(
                keyset_id=row["keyset_id"],
                epoch_date=row["epoch_date"],
                epoch_start=row["epoch_start"],
                epoch_end=row["epoch_end"],
                previous_epoch_hash=row.get("previous_epoch_hash"),
                cumulative_minted=row.get("cumulative_minted", 0),
                cumulative_burned=row.get("cumulative_burned", 0),
                total_minted=row["total_minted"],
                total_burned=row["total_burned"],
                outstanding_balance=row["outstanding_balance"],
                mint_root_hash=row["mint_root_hash"],
                mint_root_amount=row["mint_root_amount"],
                burn_root_hash=row["burn_root_hash"],
                burn_root_amount=row["burn_root_amount"],
                report_timestamp=row["report_timestamp"],
                report_hash=row["report_hash"],
                report_signature=row.get("report_signature") or "",
                ots_proof=row.get("ots_proof"),
                ots_confirmed=bool(row.get("ots_confirmed", False)),
            )
            for row in stored
        ]
        
        history = PoLHistory(keyset_id=keyset_id, epochs=epochs)
        history.chain_valid = history.verify_chain()
        
        return history
    
    async def validate_chain(self, keyset_id: str) -> ChainValidationResult:
        """Validate the entire chain of epochs for a keyset."""
        history = await self.get_epoch_history(keyset_id, limit=10000)
        
        if not history.epochs:
            return ChainValidationResult(
                keyset_id=keyset_id,
                is_valid=True,
                total_epochs=0,
            )
        
        sorted_epochs = sorted(history.epochs, key=lambda e: e.epoch_date)
        
        if sorted_epochs[0].previous_epoch_hash is not None:
            return ChainValidationResult(
                keyset_id=keyset_id,
                is_valid=False,
                total_epochs=len(sorted_epochs),
                first_epoch=sorted_epochs[0].epoch_date,
                last_epoch=sorted_epochs[-1].epoch_date,
                invalid_at_epoch=sorted_epochs[0].epoch_date,
                error_message="Genesis epoch has previous_epoch_hash set",
            )
        
        for i in range(1, len(sorted_epochs)):
            current = sorted_epochs[i]
            previous = sorted_epochs[i - 1]
            
            if not current.verify_chain_link(previous):
                return ChainValidationResult(
                    keyset_id=keyset_id,
                    is_valid=False,
                    total_epochs=len(sorted_epochs),
                    first_epoch=sorted_epochs[0].epoch_date,
                    last_epoch=sorted_epochs[-1].epoch_date,
                    invalid_at_epoch=current.epoch_date,
                    error_message=f"Chain broken: epoch {current.epoch_date} does not link to {previous.epoch_date}",
                )
        
        last = sorted_epochs[-1]
        return ChainValidationResult(
            keyset_id=keyset_id,
            is_valid=True,
            total_epochs=len(sorted_epochs),
            first_epoch=sorted_epochs[0].epoch_date,
            last_epoch=last.epoch_date,
            cumulative_minted=last.cumulative_minted,
            cumulative_burned=last.cumulative_burned,
            current_balance=last.outstanding_balance,
        )
    
    async def close_epoch(self, keyset_id: str, epoch_date: str) -> EpochReport:
        """Close an epoch, store it permanently, and submit to OpenTimestamps.
        
        Uses database locking to prevent race conditions when multiple workers
        try to close the same epoch simultaneously.
        """
        today = get_epoch_date()
        if epoch_date >= today:
            raise ValueError(
                f"Cannot close epoch {epoch_date}: only past days can be closed"
            )
        
        # Use database lock to prevent race conditions
        async with self.db.get_connection(
            lock_table="pol_reports",
            lock_select_statement=f"keyset_id='{keyset_id}' AND epoch_date='{epoch_date}'",
            lock_timeout=10,
        ) as conn:
            # Double-check inside the lock
            existing = await self.crud.get_pol_report_for_epoch(
                keyset_id=keyset_id, epoch_date=epoch_date, db=self.db, conn=conn
            )
            if existing:
                logger.warning(f"Epoch {epoch_date} already closed for keyset {keyset_id}")
                existing["report_signature"] = existing.get("report_signature") or ""
                return EpochReport(**existing)
            
            # Use fixed padding for closed epochs (k-anonymity)
            report = await self.generate_epoch_report(
                keyset_id, epoch_date, include_chain_link=True, use_fixed_padding=True
            )
            
            await self.crud.store_pol_report(
                keyset_id=report.keyset_id,
                epoch_date=report.epoch_date,
                epoch_start=report.epoch_start,
                epoch_end=report.epoch_end,
                previous_epoch_hash=report.previous_epoch_hash,
                cumulative_minted=report.cumulative_minted,
                cumulative_burned=report.cumulative_burned,
                total_minted=report.total_minted,
                total_burned=report.total_burned,
                outstanding_balance=report.outstanding_balance,
                mint_root_hash=report.mint_root_hash,
                mint_root_amount=report.mint_root_amount,
                burn_root_hash=report.burn_root_hash,
                burn_root_amount=report.burn_root_amount,
                report_timestamp=report.report_timestamp,
                report_hash=report.report_hash,
                report_signature=report.report_signature,
                expires_at=report.expires_at,
                db=self.db,
                conn=conn,
            )
        
        logger.info(
            f"Closed PoL epoch {epoch_date} for keyset {keyset_id}, "
            f"hash: {report.report_hash[:16]}..., "
            f"chain: {'genesis' if not report.previous_epoch_hash else report.previous_epoch_hash[:8] + '...'}"
        )
        
        await self._submit_to_opentimestamps(keyset_id, epoch_date, report.report_hash)
        
        return report
    
    async def _submit_to_opentimestamps(
        self, keyset_id: str, epoch_date: str, report_hash: str
    ) -> None:
        """Submit epoch hash to OpenTimestamps."""
        try:
            result = await submit_to_opentimestamps(report_hash)
            
            if result.status in (OTSStatus.PENDING, OTSStatus.COMPLETE):
                await self.crud.update_pol_report_ots(
                    keyset_id=keyset_id,
                    epoch_date=epoch_date,
                    ots_proof=result.proof,
                    ots_confirmed=(result.status == OTSStatus.COMPLETE),
                    db=self.db,
                )
                logger.info(
                    f"OTS: Submitted epoch {epoch_date} hash to OpenTimestamps "
                    f"(status: {result.status.value})"
                )
            else:
                logger.error(f"OTS submission failed: {result.error}")
                
        except Exception as e:
            logger.error(f"Failed to submit to OpenTimestamps: {e}")
    
    async def upgrade_pending_ots_proofs(self, keyset_id: str) -> int:
        """Try to upgrade all pending OTS proofs to complete."""
        unconfirmed = await self.crud.get_unconfirmed_ots_proofs(
            keyset_id=keyset_id, db=self.db
        )
        
        upgraded = 0
        for row in unconfirmed:
            if not row.get("ots_proof"):
                continue
            
            result = await upgrade_opentimestamps_proof(row["ots_proof"])
            
            if result.status == OTSStatus.COMPLETE:
                await self.crud.update_pol_report_ots(
                    keyset_id=keyset_id,
                    epoch_date=row["epoch_date"],
                    ots_proof=result.proof,
                    ots_confirmed=True,
                    db=self.db,
                )
                logger.info(
                    f"OTS: Upgraded proof for epoch {row['epoch_date']} to complete"
                )
                upgraded += 1
        
        return upgraded
    
    async def add_ots_proof(
        self, keyset_id: str, epoch_date: str, ots_proof: str,
        ots_confirmed: bool = False,
    ) -> None:
        """Add OpenTimestamps proof to a closed epoch.
        
        Args:
            ots_confirmed: Whether the proof has been verified as complete.
                           Defaults to False (pending) — use upgrade_pending_ots_proofs
                           to verify and promote to confirmed.
        """
        await self.crud.update_pol_report_ots(
            keyset_id=keyset_id,
            epoch_date=epoch_date,
            ots_proof=ots_proof,
            ots_confirmed=ots_confirmed,
            db=self.db,
        )
        
        logger.info(
            f"Added OTS proof to epoch {epoch_date} for keyset {keyset_id} "
            f"(confirmed={ots_confirmed})"
        )
    
    async def find_token_epoch(
        self, keyset_id: str, B_: str
    ) -> Optional[str]:
        """Find which epoch a token (B_) was minted in.
        
        Uses a direct DB lookup on the promises table to find the token's
        creation timestamp and derive the epoch date, avoiding O(epochs * tree_size).
        """
        row = await self.db.fetchone(
            f"""
            SELECT created FROM {self.db.table_with_schema('promises')}
            WHERE b_ = :b_ AND id = :keyset_id
            LIMIT 1
            """,
            {"b_": B_, "keyset_id": keyset_id},
        )
        
        if row:
            created = _parse_timestamp(row["created"])
            if created > 0:
                return get_epoch_date(created)
        
        today = get_epoch_date()
        status, _ = await self.get_mint_proof_inclusion(keyset_id, B_, today)
        if status == "PENDING_EPOCH":
            return today
        
        return None

    def create_mint_commitment(
        self, keyset_id: str, B_: str, amount: int
    ) -> MintCommitment:
        """Create a signed commitment promising to include a token in today's epoch.
        
        This commitment can be used by wallets to prove mint misbehavior if the
        token is not included in the closed epoch.
        """
        epoch_date = get_epoch_date()
        timestamp = int(time.time())
        
        message = f"{keyset_id}:{B_}:{amount}:{epoch_date}"
        message_hash = hashlib.sha256(message.encode()).digest()
        
        signature = self.private_key.sign_schnorr(message_hash, None)
        
        return MintCommitment(
            keyset_id=keyset_id,
            B_=B_,
            amount=amount,
            epoch_date=epoch_date,
            timestamp=timestamp,
            signature=signature.hex(),
        )

    async def prune_expired_epochs(self) -> Dict[str, int]:
        """Prune all expired epochs and their associated data.
        
        This method deletes:
        - pol_reports where expires_at < current_time
        - promises (mint proofs) for the expired time ranges
        - proofs_used (burn proofs) for the expired time ranges
        
        Returns:
            Dict with counts: reports_deleted, promises_deleted, proofs_used_deleted
        """
        if settings.pol_retention_months <= 0:
            logger.debug("Epoch pruning disabled (pol_retention_months=0)")
            return {"reports_deleted": 0, "promises_deleted": 0, "proofs_used_deleted": 0}
        
        current_time = int(time.time())
        
        expired_reports = await self.crud.get_expired_pol_reports(
            db=self.db, current_time=current_time
        )
        
        if not expired_reports:
            logger.debug("No expired epochs to prune")
            return {"reports_deleted": 0, "promises_deleted": 0, "proofs_used_deleted": 0}
        
        promises_deleted = 0
        proofs_used_deleted = 0
        
        for report in expired_reports:
            keyset_id = report["keyset_id"]
            epoch_start = report["epoch_start"]
            epoch_end = report["epoch_end"]
            epoch_date = report["epoch_date"]
            
            deleted = await self.crud.delete_promises_in_range(
                keyset_id=keyset_id,
                start_time=epoch_start,
                end_time=epoch_end,
                db=self.db,
            )
            promises_deleted += deleted
            
            deleted = await self.crud.delete_proofs_used_in_range(
                keyset_id=keyset_id,
                start_time=epoch_start,
                end_time=epoch_end,
                db=self.db,
            )
            proofs_used_deleted += deleted
            
            logger.info(
                f"Pruned epoch {epoch_date} for keyset {keyset_id}: "
                f"expired at {report['expires_at']}"
            )
        
        reports_deleted = await self.crud.delete_expired_pol_reports(
            db=self.db, current_time=current_time
        )
        
        logger.info(
            f"Epoch pruning complete: {reports_deleted} reports, "
            f"{promises_deleted} promises, {proofs_used_deleted} proofs_used deleted"
        )
        
        for report in expired_reports:
            key = (report["keyset_id"], report["epoch_date"])
            self._mint_trees.pop(key, None)
            self._burn_trees.pop(key, None)
            self._mint_data.pop(key, None)
            self._burn_data.pop(key, None)
        
        return {
            "reports_deleted": reports_deleted,
            "promises_deleted": promises_deleted,
            "proofs_used_deleted": proofs_used_deleted,
        }
