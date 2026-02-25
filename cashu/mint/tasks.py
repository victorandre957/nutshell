import asyncio
import time
from typing import List

from loguru import logger

from ..core.base import MintQuoteState
from ..core.settings import settings
from ..lightning.base import LightningBackend
from .protocols import SupportsBackends, SupportsDb, SupportsEvents


class LedgerTasks(SupportsDb, SupportsBackends, SupportsEvents):
    async def dispatch_listeners(self) -> List[asyncio.Task]:
        tasks = []
        for method, unitbackends in self.backends.items():
            for unit, backend in unitbackends.items():
                logger.debug(
                    f"Dispatching backend invoice listener for {method} {unit} {backend.__class__.__name__}"
                )
                tasks.append(asyncio.create_task(self.invoice_listener(backend)))
        
        if getattr(settings, "pol_enabled", False):
            tasks.append(asyncio.create_task(self.ots_upgrade_task()))
            tasks.append(asyncio.create_task(self.epoch_close_task()))
        
        return tasks

    async def epoch_close_task(self) -> None:
        """
        Periodically check and close past epochs that haven't been stored yet.
        This ensures historical PoL reports are persisted for auditing.
        """
        interval = getattr(settings, "pol_epoch_close_interval", 3600)  # Default: 1 hour
        lookback_days = getattr(settings, "pol_epoch_lookback_days", 30)  # Check last 30 days
        
        logger.info(f"PoL: Starting epoch close task (Interval: {interval}s, Lookback: {lookback_days} days)")
        
        await asyncio.sleep(30)

        while True:
            try:
                from datetime import datetime, timezone, timedelta
                from .pol_service import PoLService
                from ..core.proof_of_liabilities import get_epoch_date
                
                crud = getattr(self, "crud", None)
                if not crud:
                    logger.error("PoL epoch close task: crud not available")
                    await asyncio.sleep(interval)
                    continue
                    
                keysets = getattr(self, "keysets", {})
                
                pubkey_obj = getattr(self, "pubkey", None)
                mint_pubkey = pubkey_obj.format().hex() if pubkey_obj else ""
                
                seed = getattr(self, "seed", "")
                
                pol_service = PoLService(
                    db=self.db,
                    crud=crud,
                    keysets=keysets,
                    mint_pubkey=mint_pubkey,
                    seed=seed,
                )
                
                today = datetime.now(timezone.utc).date()
                
                for keyset_id in list(keysets.keys()):
                    for days_ago in range(1, lookback_days + 1):
                        epoch_date = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
                        
                        try:
                            existing = await crud.get_pol_report_for_epoch(
                                keyset_id=keyset_id, epoch_date=epoch_date, db=self.db
                            )
                            if existing:
                                continue
                            
                            report = await pol_service.generate_epoch_report(
                                keyset_id, epoch_date, include_chain_link=True
                            )
                            
                            if report.total_minted > 0 or report.total_burned > 0:
                                await pol_service.close_epoch(keyset_id, epoch_date)
                                logger.info(
                                    f"PoL: Auto-closed epoch {epoch_date} for keyset {keyset_id} "
                                    f"(minted: {report.total_minted}, burned: {report.total_burned})"
                                )
                        except Exception as e:
                            logger.debug(f"PoL: Could not close epoch {epoch_date} for {keyset_id}: {e}")
                            
            except Exception as e:
                logger.error(f"PoL epoch close task error: {e}")
            
            await asyncio.sleep(interval)

    async def ots_upgrade_task(self) -> None:
        """
        Periodically upgrade pending OTS proofs to complete.
        Background task that runs forever.
        """
        interval = getattr(settings, "pol_ots_upgrade_interval", 3600)
        logger.info(f"OTS: Starting upgrade task (Interval: {interval}s)")
        
        await asyncio.sleep(10)

        while True:
            try:
                from .pol_service import PoLService
                
                crud = getattr(self, "crud", None)
                if not crud:
                    logger.error("OTS upgrade task: crud not available")
                    await asyncio.sleep(interval)
                    continue
                    
                keysets = getattr(self, "keysets", {})
                
                pubkey_obj = getattr(self, "pubkey", None)
                mint_pubkey = pubkey_obj.format().hex() if pubkey_obj else ""
                
                seed = getattr(self, "seed", "")
                
                pol_service = PoLService(
                    db=self.db,
                    crud=crud,
                    keysets=keysets,
                    mint_pubkey=mint_pubkey,
                    seed=seed,
                )
                
                for keyset_id in list(keysets.keys()):
                    upgraded = await pol_service.upgrade_pending_ots_proofs(keyset_id)
                    if upgraded > 0:
                        logger.info(f"OTS: Successfully upgraded {upgraded} proofs for keyset {keyset_id}")
                        
            except Exception as e:
                logger.error(f"OTS upgrade task error: {e}")
            
            await asyncio.sleep(interval)

    async def invoice_listener(self, backend: LightningBackend) -> None:
        if backend.supports_incoming_payment_stream:
            retry_delay = settings.mint_retry_exponential_backoff_base_delay
            max_retry_delay = settings.mint_retry_exponential_backoff_max_delay
            
            while True:
                try:
                    # Reset retry delay on successful connection to backend stream
                    retry_delay = settings.mint_retry_exponential_backoff_base_delay
                    async for checking_id in backend.paid_invoices_stream():
                        await self.invoice_callback_dispatcher(checking_id)
                except Exception as e:
                    logger.error(f"Error in invoice listener: {e}")
                    logger.info(f"Restarting invoice listener in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    
                    # Exponential backoff
                    retry_delay = min(retry_delay * 2, max_retry_delay)

    async def invoice_callback_dispatcher(self, checking_id: str) -> None:
        logger.debug(f"Invoice callback dispatcher: {checking_id}")
        async with self.db.get_connection(
            lock_table="mint_quotes",
            lock_select_statement=f"checking_id='{checking_id}'",
            lock_timeout=5,
        ) as conn:
            quote = await self.crud.get_mint_quote(
                checking_id=checking_id, db=self.db, conn=conn
            )
            if not quote:
                logger.error(f"Quote not found for {checking_id}")
                return

            logger.trace(
                f"Invoice callback dispatcher: quote {quote} trying to set as {MintQuoteState.paid}"
            )
            # set the quote as paid
            if quote.unpaid:
                quote.state = MintQuoteState.paid
                quote.paid_time = int(time.time())
                await self.crud.update_mint_quote(quote=quote, db=self.db, conn=conn)
                logger.trace(
                    f"Quote {quote.quote} with {MintQuoteState.unpaid} set as {quote.state.value}"
                )

        await self.events.submit(quote)
