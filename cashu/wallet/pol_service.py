import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from ..core.crypto.b_dhke import hash_to_curve
from ..core.db import Database
from ..core.proof_of_liabilities import (
    MerkleSumProof,
    MerkleSumTree,
    WalletPoLRecord,
    get_epoch_date,
)
from . import crud


class WalletPoLService:
    """Wallet-side service for verifying mint's Proof of Liabilities."""
    
    def __init__(self, db: Database):
        self.db = db
        self._verification_task: Optional[asyncio.Task] = None
        self._verification_interval = 3600

    @staticmethod
    def verify_merkle_proof_locally(proof_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Verify a Merkle Sum proof locally by recalculating the path.
        
        Args:
            proof_data: Dict containing leaf_value, leaf_amount, siblings, root_hash, root_amount
            
        Returns:
            Tuple of (is_valid, message)
        """
        if not proof_data:
            return False, "No proof data provided"
        
        try:
            proof = MerkleSumProof(
                leaf_value=proof_data["leaf_value"],
                leaf_amount=proof_data["leaf_amount"],
                siblings=[tuple(s) for s in proof_data["siblings"]],
                root_hash=proof_data["root_hash"],
                root_amount=proof_data["root_amount"],
            )
            
            is_valid = proof.verify()
            
            if is_valid:
                return True, f"Proof verified: leaf={proof.leaf_value[:16]}..., amount={proof.leaf_amount}, root_amount={proof.root_amount}"
            else:
                return False, f"Proof INVALID: computed root does not match. Expected {proof.root_hash[:16]}..."
                
        except Exception as e:
            return False, f"Proof verification failed: {str(e)}"

    @staticmethod
    def verify_proof_step_by_step(proof_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Verify a Merkle proof step by step, showing intermediate hashes.
        Useful for debugging and educational purposes.
        
        Returns detailed verification steps.
        """
        import hashlib
        
        def _sha256(data: str) -> str:
            return hashlib.sha256(data.encode()).hexdigest()
        
        result = {
            "valid": False,
            "steps": [],
            "final_hash": None,
            "final_amount": None,
            "expected_root_hash": proof_data.get("root_hash"),
            "expected_root_amount": proof_data.get("root_amount"),
        }
        
        if not proof_data:
            result["error"] = "No proof data"
            return result
        
        try:
            leaf_value = proof_data["leaf_value"]
            leaf_amount = proof_data["leaf_amount"]
            siblings = proof_data["siblings"]
            
            leaf_preimage = f"{leaf_value}:{leaf_amount}"
            current_hash = _sha256(leaf_preimage)
            current_amount = leaf_amount
            
            result["steps"].append({
                "step": 0,
                "type": "leaf",
                "preimage": leaf_preimage,
                "hash": current_hash,
                "amount": current_amount,
            })
            
            for i, sibling in enumerate(siblings):
                sibling_hash, sibling_amount, position = sibling
                total_amount = current_amount + sibling_amount
                
                if position == "L":
                    preimage = f"{sibling_hash}:{current_hash}:{total_amount}"
                else:
                    preimage = f"{current_hash}:{sibling_hash}:{total_amount}"
                
                new_hash = _sha256(preimage)
                
                result["steps"].append({
                    "step": i + 1,
                    "type": "internal",
                    "sibling_hash": sibling_hash[:16] + "...",
                    "sibling_amount": sibling_amount,
                    "sibling_position": position,
                    "preimage_pattern": f"{'sibling' if position == 'L' else 'current'}:{{'current' if position == 'L' else 'sibling'}}:{total_amount}",
                    "hash": new_hash,
                    "amount": total_amount,
                })
                
                current_hash = new_hash
                current_amount = total_amount
            
            result["final_hash"] = current_hash
            result["final_amount"] = current_amount
            result["valid"] = (
                current_hash == proof_data["root_hash"] and
                current_amount == proof_data["root_amount"]
            )
            
        except Exception as e:
            result["error"] = str(e)
        
        return result

    async def store_mint_record(
        self,
        keyset_id: str,
        B_: str,
        C_: str,
        amount: int,
        secret: str,
        dleq_e: Optional[str] = None,
        dleq_s: Optional[str] = None,
        dleq_r: Optional[str] = None,
    ) -> None:
        """Store a record of minted token for later verification."""
        Y = hash_to_curve(secret.encode("utf-8")).format().hex()
        await crud.store_pol_record(
            db=self.db,
            keyset_id=keyset_id,
            B_=B_,
            C_=C_,
            amount=amount,
            secret=secret,
            Y=Y,
            created=int(time.time()),
            dleq_e=dleq_e,
            dleq_s=dleq_s,
            dleq_r=dleq_r,
        )

    async def mark_as_burned(self, secret: str) -> None:
        """Mark a token as burned (spent)."""
        Y = hash_to_curve(secret.encode("utf-8")).format().hex()
        await crud.mark_pol_record_burned(db=self.db, Y=Y)

    async def get_records(
        self, keyset_id: Optional[str] = None, burned: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """Get stored PoL records."""
        return await crud.get_pol_records(db=self.db, keyset_id=keyset_id, burned=burned)

    async def verify_with_api(
        self,
        api_client,
        keyset_id: str,
        verify_proofs_locally: bool = True,
    ) -> Dict[str, Any]:
        """Verify wallet records against mint's PoL report.
        
        Args:
            api_client: API client instance
            keyset_id: Keyset ID to verify
            verify_proofs_locally: If True, also verify Merkle proofs locally (recommended)
        """
        results = {
            "keyset_id": keyset_id,
            "epoch_date": get_epoch_date(),
            "timestamp": int(time.time()),
            "mint_verified": [],
            "mint_pending": [],
            "mint_missing": [],
            "mint_proof_invalid": [],
            "burn_verified": [],
            "burn_pending": [],
            "burn_missing": [],
            "is_valid": True,
            "local_verification": verify_proofs_locally,
            "alerts": [],
        }
        
        records = await self.get_records(keyset_id=keyset_id)
        
        for record in records:
            if record.get("burned"):
                continue
            B_ = record["B_"]
            record_epoch = get_epoch_date(record.get("created"))
            try:
                resp = await api_client.verify_pol_mint(keyset_id, B_, epoch_date=record_epoch)
                status = resp.get("status", "NOT_FOUND")
                proof = resp.get("proof")
                
                if status == "INCLUDED":
                    if verify_proofs_locally and proof:
                        is_valid, msg = self.verify_merkle_proof_locally(proof)
                        if is_valid:
                            results["mint_verified"].append({
                                "B_": B_,
                                "epoch": record_epoch,
                                "local_verified": True,
                            })
                        else:
                            results["mint_proof_invalid"].append(B_)
                            results["is_valid"] = False
                            results["alerts"].append(
                                f"ALERT: Token {B_[:16]}... proof verification FAILED: {msg}"
                            )
                    else:
                        results["mint_verified"].append({
                            "B_": B_,
                            "epoch": record_epoch,
                            "local_verified": False,
                        })
                elif status == "PENDING_EPOCH":
                    results["mint_pending"].append(B_)
                else: 
                    results["mint_missing"].append(B_)
                    results["is_valid"] = False
                    results["alerts"].append(
                        f"ALERT: Token {B_[:16]}... not in mint tree (epoch {record_epoch})!"
                    )
            except Exception as e:
                logger.warning(f"Failed to verify mint {B_[:16]}...: {e}")
        
        for record in records:
            if not record.get("burned"):
                continue
            Y = record["Y"]
            burned_at = record.get("burned_at") or record.get("created")
            record_epoch = get_epoch_date(burned_at)
            try:
                resp = await api_client.verify_pol_burn(keyset_id, Y, epoch_date=record_epoch)
                status = resp.get("status", "NOT_FOUND")
                proof = resp.get("proof")
                
                if status == "INCLUDED":
                    if verify_proofs_locally and proof:
                        is_valid, msg = self.verify_merkle_proof_locally(proof)
                        if is_valid:
                            results["burn_verified"].append({
                                "Y": Y,
                                "epoch": record_epoch,
                                "local_verified": True,
                            })
                        else:
                            results["is_valid"] = False
                            results["alerts"].append(
                                f"ALERT: Burn proof for {Y[:16]}... verification FAILED: {msg}"
                            )
                    else:
                        results["burn_verified"].append({
                            "Y": Y,
                            "epoch": record_epoch,
                            "local_verified": False,
                        })
                elif status == "PENDING_EPOCH":
                    results["burn_pending"].append(Y)
                else:
                    results["burn_missing"].append(Y)
            except Exception as e:
                logger.warning(f"Failed to verify burn {Y[:16]}...: {e}")
        
        try:
            latest_epoch = get_epoch_date()
            if records:
                timestamps = [r.get("burned_at") or r.get("created") for r in records]
                latest_ts = max(t for t in timestamps if t)
                latest_epoch = get_epoch_date(latest_ts)
            
            roots = await api_client.get_pol_roots(keyset_id, epoch_date=latest_epoch)
            results["reported_outstanding"] = roots.get("outstanding_balance", 0)
            results["checked_epoch"] = latest_epoch
            
            unburned = [r for r in records if not r.get("burned")]
            wallet_balance = sum(r["amount"] for r in unburned)
            results["wallet_balance"] = wallet_balance
            
            if wallet_balance > results["reported_outstanding"]:
                results["is_valid"] = False
                results["alerts"].append(
                    f"ALERT: Wallet balance ({wallet_balance}) > "
                    f"reported outstanding ({results['reported_outstanding']}) "
                    f"on epoch {latest_epoch}"
                )
        except Exception as e:
            logger.warning(f"Failed to get roots: {e}")
        
        return results

    async def start_periodic_verification(
        self,
        api_client,
        keyset_ids: List[str],
        interval: int = 3600,
        callback: Optional[callable] = None,
    ) -> None:
        """Start background task to verify PoL periodically."""
        self._verification_interval = interval
        
        async def verification_loop():
            while True:
                for keyset_id in keyset_ids:
                    try:
                        result = await self.verify_with_api(api_client, keyset_id)
                        if not result["is_valid"]:
                            logger.warning(
                                f"PoL verification failed for {keyset_id}: "
                                f"{result['alerts']}"
                            )
                            if callback:
                                await callback(result)
                    except Exception as e:
                        logger.error(f"PoL verification error for {keyset_id}: {e}")
                
                await asyncio.sleep(self._verification_interval)
        
        if self._verification_task:
            self._verification_task.cancel()
        
        self._verification_task = asyncio.create_task(verification_loop())
        logger.info(f"Started PoL verification (interval: {interval}s)")

    def stop_periodic_verification(self) -> None:
        """Stop background verification task."""
        if self._verification_task:
            self._verification_task.cancel()
            self._verification_task = None
            logger.info("Stopped PoL verification")
