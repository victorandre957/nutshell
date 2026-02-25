import asyncio
import base64
import io
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from loguru import logger

try:
    from opentimestamps.core.timestamp import Timestamp, DetachedTimestampFile
    from opentimestamps.core.op import OpSHA256
    from opentimestamps.core.notary import (
        BitcoinBlockHeaderAttestation,
        PendingAttestation,
    )
    from opentimestamps.core.serialize import (
        StreamSerializationContext,
        StreamDeserializationContext,
    )
    import opentimestamps.calendar
    OTS_AVAILABLE = True
except ImportError:
    OTS_AVAILABLE = False
    logger.warning("opentimestamps library not installed. OTS features disabled.")


class OTSStatus(Enum):
    """Status of an OpenTimestamps proof."""
    PENDING = "pending"
    COMPLETE = "complete"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass
class OTSResult:
    """Result from OTS operation."""
    status: OTSStatus
    proof: Optional[str] = None
    proof_bytes: Optional[bytes] = None
    bitcoin_height: Optional[int] = None
    error: Optional[str] = None


OTS_CALENDARS = [
    "https://a.pool.opentimestamps.org",
    "https://b.pool.opentimestamps.org",
    "https://a.pool.eternitywall.com",
    "https://ots.btc.catallaxy.com",
]


def _remote_calendar(url: str):
    """Create a remote calendar client."""
    return opentimestamps.calendar.RemoteCalendar(url)


class OpenTimestampsClient:
    """Client for OpenTimestamps operations."""
    
    def __init__(
        self,
        calendars: list[str] = None,
        timeout: float = 30.0,
        enabled: bool = True,
    ):
        self.calendars = calendars or OTS_CALENDARS
        self.timeout = timeout
        self.enabled = enabled and OTS_AVAILABLE
        self._executor = ThreadPoolExecutor(max_workers=2)
    
    def _stamp_sync(self, digest: bytes) -> Optional[bytes]:
        """Create a timestamp for a digest (synchronous). Returns .ots file bytes."""
        if not OTS_AVAILABLE:
            return None
        
        timestamp = Timestamp(digest)
        
        submitted = False
        for calendar_url in self.calendars:
            try:
                calendar = _remote_calendar(calendar_url)
                result = calendar.submit(digest, timeout=int(self.timeout))
                if result is not None:
                    timestamp.merge(result)
                    logger.info(f"OTS: Submitted to {calendar_url}")
                    submitted = True
                    break
            except Exception as e:
                logger.warning(f"Calendar {calendar_url} failed: {e}")
                continue
        
        if not submitted:
            logger.error("OTS: All calendars failed")
            return None
        
        try:
            file_timestamp = DetachedTimestampFile(OpSHA256(), timestamp)
            ctx_buffer = io.BytesIO()
            ctx = StreamSerializationContext(ctx_buffer)
            file_timestamp.serialize(ctx)
            return ctx_buffer.getvalue()
        except Exception as e:
            logger.error(f"Failed to serialize timestamp: {e}")
            return None
    
    def _upgrade_sync(self, proof_bytes: bytes) -> tuple[bytes, bool]:
        """Upgrade a timestamp proof (synchronous)."""
        if not OTS_AVAILABLE:
            return proof_bytes, False
        
        try:
            ctx = StreamDeserializationContext(io.BytesIO(proof_bytes))
            detached_timestamp = DetachedTimestampFile.deserialize(ctx)
            
            changed = False
            
            for calendar_url in self.calendars:
                try:
                    calendar = _remote_calendar(calendar_url)
                    
                    for msg, attestation in list(detached_timestamp.timestamp.all_attestations()):
                        if isinstance(attestation, PendingAttestation):
                            try:
                                upgraded = calendar.get_timestamp(msg)
                                if upgraded is not None:
                                    self._merge_attestation(
                                        detached_timestamp.timestamp,
                                        msg,
                                        upgraded
                                    )
                                    changed = True
                                    logger.info(f"OTS: Got attestation from {calendar_url}")
                            except Exception:
                                pass
                except Exception as e:
                    logger.debug(f"Upgrade via {calendar_url} failed: {e}")
                    continue
            
            is_complete = self._is_timestamp_complete(detached_timestamp.timestamp)
            
            if changed:
                ctx_buffer = io.BytesIO()
                ctx = StreamSerializationContext(ctx_buffer)
                detached_timestamp.serialize(ctx)
                return ctx_buffer.getvalue(), is_complete
            
            return proof_bytes, is_complete
            
        except Exception as e:
            logger.error(f"Failed to upgrade timestamp: {e}")
            return proof_bytes, False
    
    def _merge_attestation(self, timestamp: "Timestamp", target_msg: bytes, new_timestamp: "Timestamp"):
        """Merge a new timestamp into the existing one at the matching message."""
        if timestamp.msg == target_msg:
            timestamp.merge(new_timestamp)
            return True
        
        for op, result in timestamp.ops.items():
            if self._merge_attestation(result, target_msg, new_timestamp):
                return True
        
        return False
    
    def _is_timestamp_complete(self, timestamp: "Timestamp") -> bool:
        """Check if timestamp has Bitcoin blockchain attestation."""
        if not OTS_AVAILABLE:
            return False
        
        for msg, attestation in timestamp.all_attestations():
            if isinstance(attestation, BitcoinBlockHeaderAttestation):
                return True
        return False
    
    def _get_bitcoin_height(self, timestamp: "Timestamp") -> Optional[int]:
        """Get Bitcoin block height from attestation if available."""
        if not OTS_AVAILABLE:
            return None
        
        for msg, attestation in timestamp.all_attestations():
            if isinstance(attestation, BitcoinBlockHeaderAttestation):
                return attestation.height
        return None
    
    async def submit_hash(self, hash_hex: str) -> OTSResult:
        """Submit a hash to OpenTimestamps calendars."""
        if not self.enabled:
            logger.debug("OTS disabled, skipping submission")
            return OTSResult(
                status=OTSStatus.FAILED,
                error="OTS disabled or library not available",
            )
        
        try:
            digest = bytes.fromhex(hash_hex)
        except ValueError:
            return OTSResult(
                status=OTSStatus.FAILED,
                error="Invalid hash format",
            )
        
        loop = asyncio.get_event_loop()
        proof_bytes = await loop.run_in_executor(
            self._executor,
            self._stamp_sync,
            digest
        )
        
        if proof_bytes is None:
            return OTSResult(
                status=OTSStatus.FAILED,
                error="Failed to create timestamp",
            )
        
        proof_b64 = base64.b64encode(proof_bytes).decode()
        logger.info(f"OTS: Created timestamp for {hash_hex[:16]}... ({len(proof_bytes)} bytes)")
        
        return OTSResult(
            status=OTSStatus.PENDING,
            proof=proof_b64,
            proof_bytes=proof_bytes,
        )
    
    async def upgrade_proof(self, proof_b64: str) -> OTSResult:
        """Try to upgrade a pending proof to Bitcoin-anchored."""
        if not self.enabled:
            return OTSResult(
                status=OTSStatus.PENDING,
                proof=proof_b64,
                error="OTS disabled",
            )
        
        try:
            proof_bytes = base64.b64decode(proof_b64)
        except Exception:
            return OTSResult(
                status=OTSStatus.FAILED,
                error="Invalid proof encoding",
            )
        
        loop = asyncio.get_event_loop()
        upgraded_bytes, is_complete = await loop.run_in_executor(
            self._executor,
            self._upgrade_sync,
            proof_bytes
        )
        
        upgraded_b64 = base64.b64encode(upgraded_bytes).decode()
        
        if is_complete:
            bitcoin_height = None
            try:
                ctx = StreamDeserializationContext(io.BytesIO(upgraded_bytes))
                dt = DetachedTimestampFile.deserialize(ctx)
                bitcoin_height = self._get_bitcoin_height(dt.timestamp)
            except Exception:
                pass
            
            logger.info(f"OTS: Proof upgraded to complete (block {bitcoin_height})")
            return OTSResult(
                status=OTSStatus.COMPLETE,
                proof=upgraded_b64,
                proof_bytes=upgraded_bytes,
                bitcoin_height=bitcoin_height,
            )
        
        return OTSResult(
            status=OTSStatus.PENDING,
            proof=upgraded_b64,
            proof_bytes=upgraded_bytes,
        )
    
    def close(self):
        """Cleanup resources."""
        self._executor.shutdown(wait=False)


_ots_client: Optional[OpenTimestampsClient] = None


def get_ots_client() -> OpenTimestampsClient:
    """Get global OTS client."""
    global _ots_client
    if _ots_client is None:
        try:
            from ..core.settings import settings
            enabled = getattr(settings, "pol_opentimestamps_enabled", True)
        except Exception:
            enabled = True
        _ots_client = OpenTimestampsClient(enabled=enabled)
    return _ots_client


async def submit_to_opentimestamps(hash_hex: str) -> OTSResult:
    """Submit a hash to OpenTimestamps. Returns .ots file."""
    client = get_ots_client()
    return await client.submit_hash(hash_hex)


async def upgrade_opentimestamps_proof(proof_b64: str) -> OTSResult:
    """Try to upgrade a pending OTS proof."""
    client = get_ots_client()
    return await client.upgrade_proof(proof_b64)


def decode_ots_proof(proof_b64: str) -> Optional[bytes]:
    """Decode base64 OTS proof to raw bytes for download."""
    try:
        return base64.b64decode(proof_b64)
    except Exception:
        return None
