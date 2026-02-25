import asyncio
import hashlib
import time
import pytest
import base64
from unittest.mock import AsyncMock, MagicMock, patch

from cashu.core.proof_of_liabilities import (
    MerkleSumTree,
    MerkleSumProof,
    EpochReport,
    PoLHistory,
)

from cashu.mint.opentimestamps import (
    OTSStatus,
    decode_ots_proof,
    OTS_CALENDARS,
    OpenTimestampsClient,
    OTSResult,
)

from cashu.mint.pol_service import PoLService
from cashu.mint.tasks import LedgerTasks

class TestMerkleSumTree:
    """Tests for MerkleSumTree core functionality."""
    
    def test_empty_tree(self):
        from cashu.core.proof_of_liabilities import MerkleSumTree
        tree = MerkleSumTree([], epoch_date="2024-01-01")
        assert tree.total_amount == 0
    
    def test_single_leaf(self):
        tree = MerkleSumTree([("a", 100)], epoch_date="2024-01-01")
        assert tree.total_amount == 100
        proof = tree.get_proof("a")
        assert proof is not None and proof.verify()
    
    def test_multiple_leaves(self):
        tree = MerkleSumTree([("a", 10), ("b", 20), ("c", 30), ("d", 40)], epoch_date="2024-01-01")
        assert tree.total_amount == 100
        for value, amount in [("a", 10), ("b", 20), ("c", 30), ("d", 40)]:
            proof = tree.get_proof(value)
            assert proof and proof.verify() and proof.leaf_amount == amount
    
    def test_nonexistent_leaf_returns_none(self):
        tree = MerkleSumTree([("a", 100)], epoch_date="2024-01-01")
        assert tree.get_proof("x") is None
    
    def test_tampering_detected(self):
        tree = MerkleSumTree([("t1", 100), ("t2", 200)], epoch_date="2024-01-01")
        proof = tree.get_proof("t1")
        proof.root_amount = 250
        assert not proof.verify()
    
    def test_deterministic(self):
        data = [("x", 50), ("y", 100)]
        t1 = MerkleSumTree(data, epoch_date="2024-01-01")
        t2 = MerkleSumTree(data, epoch_date="2024-01-01")
        assert t1.root_hash == t2.root_hash


class TestTreePadding:
    """Tests for tree padding to power of 2."""
    
    def test_single_leaf_padded_size(self):
        tree = MerkleSumTree([("a", 1)], epoch_date="2024-01-01")
        assert tree.padded_size == 1
    
    def test_two_leaves_padded_size(self):
        tree = MerkleSumTree([("a", 1), ("b", 2)], epoch_date="2024-01-01")
        assert tree.padded_size == 2
    
    def test_three_leaves_padded_to_four(self):
        tree = MerkleSumTree([("a", 1), ("b", 2), ("c", 3)], epoch_date="2024-01-01")
        assert tree.padded_size == 4


class TestEpochChaining:
    """Tests for epoch chaining."""
    
    def test_genesis_validates(self):
        e = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            previous_epoch_hash=None, cumulative_minted=100, cumulative_burned=10,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a", mint_root_amount=100, burn_root_hash="b", burn_root_amount=10,
            report_timestamp=int(time.time()), report_hash="", report_signature="",
        )
        e.report_hash = e.compute_hash()
        assert e.verify_chain_link(None)
    
    def test_chain_link_valid(self):
        e1 = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            previous_epoch_hash=None, cumulative_minted=100, cumulative_burned=10,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a", mint_root_amount=100, burn_root_hash="b", burn_root_amount=10,
            report_timestamp=int(time.time()), report_hash="", report_signature="",
        )
        e1.report_hash = e1.compute_hash()
        
        e2 = EpochReport(
            keyset_id="t", epoch_date="2024-01-02", epoch_start=1704153600, epoch_end=1704239999,
            previous_epoch_hash=e1.report_hash, cumulative_minted=200, cumulative_burned=30,
            total_minted=100, total_burned=20, outstanding_balance=170,
            mint_root_hash="c", mint_root_amount=100, burn_root_hash="d", burn_root_amount=20,
            report_timestamp=int(time.time()), report_hash="", report_signature="",
        )
        e2.report_hash = e2.compute_hash()
        assert e2.verify_chain_link(e1)


class TestPoLHistory:
    """Tests for chain validation."""
    
    def test_valid_chain(self):
        e1 = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            previous_epoch_hash=None, cumulative_minted=100, cumulative_burned=10,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a", mint_root_amount=100, burn_root_hash="b", burn_root_amount=10,
            report_timestamp=int(time.time()), report_hash="", report_signature="",
        )
        e1.report_hash = e1.compute_hash()
        history = PoLHistory(keyset_id="t", epochs=[e1])
        assert history.verify_chain()
    
    def test_broken_chain(self):
        e1 = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            previous_epoch_hash=None, cumulative_minted=100, cumulative_burned=10,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a", mint_root_amount=100, burn_root_hash="b", burn_root_amount=10,
            report_timestamp=int(time.time()), report_hash="", report_signature="",
        )
        e1.report_hash = e1.compute_hash()
        e2 = EpochReport(
            keyset_id="t", epoch_date="2024-01-02", epoch_start=1704153600, epoch_end=1704239999,
            previous_epoch_hash="wrong_hash", cumulative_minted=200, cumulative_burned=30,
            total_minted=100, total_burned=20, outstanding_balance=170,
            mint_root_hash="c", mint_root_amount=100, burn_root_hash="d", burn_root_amount=20,
            report_timestamp=int(time.time()), report_hash="", report_signature="",
        )
        e2.report_hash = e2.compute_hash()
        history = PoLHistory(keyset_id="t", epochs=[e1, e2])
        assert not history.verify_chain()


class TestEpochReport:
    """Tests for EpochReport."""
    
    def test_hash_deterministic(self):
        r = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a", mint_root_amount=100, burn_root_hash="b", burn_root_amount=10,
            report_timestamp=1704067200, report_hash="", report_signature="",
        )
        assert r.compute_hash() == r.compute_hash()
        assert len(r.compute_hash()) == 64
    
    def test_ots_fields_default(self):
        r = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a", mint_root_amount=100, burn_root_hash="b", burn_root_amount=10,
            report_timestamp=int(time.time()), report_hash="h", report_signature="",
        )
        assert r.ots_confirmed == False and r.ots_proof is None


class TestMerkleSumProofSerialization:
    """Tests for proof serialization."""
    
    def test_json_roundtrip(self):
        tree = MerkleSumTree([("a", 100), ("b", 200)], epoch_date="2024-01-01")
        proof = tree.get_proof("b")
        restored = MerkleSumProof.model_validate_json(proof.model_dump_json())
        assert restored.verify() and restored.leaf_amount == 200


class TestOpenTimestamps:
    """Tests for OpenTimestamps."""
    
    def test_status_enum(self):
        assert OTSStatus.PENDING.value == "pending"
        assert OTSStatus.COMPLETE.value == "complete"
    
    def test_decode_proof(self):
        original = b"test"
        assert decode_ots_proof(base64.b64encode(original).decode()) == original
        assert decode_ots_proof("invalid!!!") is None
    
    def test_calendars_config(self):
        assert len(OTS_CALENDARS) > 0
        client = OpenTimestampsClient(calendars=["https://custom.com"])
        assert client.calendars == ["https://custom.com"]
    
    def test_disabled_client(self):
        client = OpenTimestampsClient(enabled=False)
        result = asyncio.get_event_loop().run_until_complete(client.submit_hash("a" * 64))
        assert result.status == OTSStatus.FAILED
    
    def test_invalid_hash(self):
        client = OpenTimestampsClient(enabled=True)
        result = asyncio.get_event_loop().run_until_complete(client.submit_hash("not_hex"))
        assert result.status == OTSStatus.FAILED


class TestPoLService:
    """Tests for PoLService."""
    
    def test_init(self):
        s = PoLService(db=MagicMock(), crud=MagicMock(), keysets={"k": MagicMock()}, mint_pubkey="02"+"a"*64, seed="t")
        assert "k" in s.keysets
    
    def test_epoch_generation(self):
        crud = MagicMock()
        crud.get_mint_proofs_in_range = AsyncMock(return_value=[{"keyset_id": "k", "amount": 100, "b_": "B", "c_": "C", "created": 1704067200}])
        crud.get_burn_proofs_in_range = AsyncMock(return_value=[])
        crud.get_pol_reports_for_keyset = AsyncMock(return_value=[])
        s = PoLService(db=MagicMock(), crud=crud, keysets={"k": MagicMock()}, mint_pubkey="02"+"a"*64, seed="t")
        r = asyncio.get_event_loop().run_until_complete(s.generate_epoch_report("k", "2024-01-01"))
        assert r.total_minted == 100 and len(r.report_hash) == 64
    
    def test_close_epoch_rejects_today(self):
        from cashu.core.proof_of_liabilities import get_epoch_date
        s = PoLService(db=MagicMock(), crud=MagicMock(), keysets={"k": MagicMock()}, mint_pubkey="02"+"a"*64, seed="t")
        with pytest.raises(ValueError, match="past days"):
            asyncio.get_event_loop().run_until_complete(s.close_epoch("k", get_epoch_date()))


class TestLedgerTasks:
    """Tests for background tasks."""
    
    def test_ots_upgrade_task_exists(self):
        assert hasattr(LedgerTasks, 'ots_upgrade_task')
    
    def test_dispatch_checks_pol(self):
        import inspect
        assert 'pol_enabled' in inspect.getsource(LedgerTasks.dispatch_listeners)


class TestEfficiency:
    """Tests for efficiency."""
    
    def test_proof_size_log_n(self):
        depths = []
        for n in [10, 100, 1000]:
            tree = MerkleSumTree([(f"t{i}", i) for i in range(n)], epoch_date="2024-01-01")
            depths.append(len(tree.get_proof(f"t{n//2}").siblings))
        assert depths[0] <= 5 and depths[1] <= 8 and depths[2] <= 11

class TestOTSWorkflow:
    """
    Tests the full OpenTimestamps lifecycle (Submit -> Pending -> Upgrade -> Complete)
    using a Mock client to simulate the server response without needing Bitcoin.
    """
    
    def test_ots_lifecycle_logic(self):
        class MockOTSClient:
            def __init__(self):
                self.server_state = {}

            async def submit_hash(self, file_bytes: bytes):
                proof_content = b"PENDING_" + file_bytes
                proof_b64 = base64.b64encode(proof_content).decode()
                self.server_state[proof_b64] = "PENDING"
                return OTSResult(status=OTSStatus.PENDING, proof=proof_b64)

            async def upgrade_proof(self, proof_b64: str):
                if self.server_state.get(proof_b64) == "PENDING":
                    raw = base64.b64decode(proof_b64)
                    upgraded = raw + b"_ANCHORED"
                    upgraded_b64 = base64.b64encode(upgraded).decode()
                    self.server_state[upgraded_b64] = "COMPLETE"
                    return OTSResult(status=OTSStatus.COMPLETE, proof=upgraded_b64)
                return OTSResult(status=OTSStatus.PENDING, proof=proof_b64)

        async def run_workflow():
            client = MockOTSClient()
            data_hash = hashlib.sha256(b"relatorio_diario").digest()
            
            result_sub = await client.submit_hash(data_hash)
            assert result_sub.status.value == "pending"
            pending_proof = result_sub.proof
            
            result_upg = await client.upgrade_proof(pending_proof)
            assert result_upg.status.value == "complete"
            assert result_upg.proof != pending_proof
            assert "ANCHORED" in str(base64.b64decode(result_upg.proof))
            
            print("  ✓ OTS Workflow logic (Mock) passed")

        asyncio.run(run_workflow())


class TestReportSignature:
    """Tests for report signature generation and verification."""
    
    def test_report_signature_field_required(self):
        """EpochReport.report_signature is now a required field."""
        with pytest.raises(Exception):
            EpochReport(
                keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
                total_minted=100, total_burned=10, outstanding_balance=90,
                mint_root_hash="a" * 64, mint_root_amount=100, burn_root_hash="b" * 64, burn_root_amount=10,
                report_timestamp=1704067200, report_hash="h" * 64,
            )
    
    def test_verify_report_signature_empty_returns_false(self):
        """Empty signature should return False from verify_report_signature."""
        report = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a" * 64, mint_root_amount=100, burn_root_hash="b" * 64, burn_root_amount=10,
            report_timestamp=1704067200, report_hash="a" * 64, report_signature="",
        )
        assert not report.verify_report_signature("02" + "a" * 64)
    
    def test_verify_report_signature_method_exists(self):
        """EpochReport should have verify_report_signature method."""
        report = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a" * 64, mint_root_amount=100, burn_root_hash="b" * 64, burn_root_amount=10,
            report_timestamp=1704067200, report_hash="h", report_signature="s",
        )
        assert hasattr(report, "verify_report_signature")
        assert callable(report.verify_report_signature)


class TestShuffledTreeIndices:
    """Tests for shuffled tree indices after padding."""
    
    def test_leaf_index_tracks_shuffled_position(self):
        """Leaf indices should track position after shuffling."""
        tokens = [("a", 100), ("b", 200), ("c", 300), ("d", 400)]
        tree = MerkleSumTree(tokens, epoch_date="2024-01-01")
        
        for token, amount in tokens:
            proof = tree.get_proof(token)
            assert proof is not None, f"Proof for {token} should exist"
            assert proof.verify(), f"Proof for {token} should verify"
            assert proof.leaf_amount == amount, f"Amount for {token} should be {amount}"
    
    def test_padded_tree_proof_verification(self):
        """Proofs should verify correctly in padded trees."""
        tokens = [("x", 1), ("y", 2), ("z", 3)]
        tree = MerkleSumTree(tokens, epoch_date="2024-01-01")
        
        assert tree.padded_size == 4
        assert tree.total_amount == 6
        
        for token, amount in tokens:
            proof = tree.get_proof(token)
            assert proof.verify(), f"Proof for {token} should verify"
    
    def test_large_padded_tree(self):
        """Large padded trees should work correctly."""
        tokens = [(f"token_{i}", i) for i in range(1, 101)]
        tree = MerkleSumTree(tokens, epoch_date="2024-01-01")
        
        assert tree.padded_size == 128
        expected_total = sum(i for i in range(1, 101))
        assert tree.total_amount == expected_total
        
        for i in [1, 50, 100]:
            proof = tree.get_proof(f"token_{i}")
            assert proof is not None
            assert proof.verify()
            assert proof.leaf_amount == i


class TestEpochExpiration:
    """Tests for epoch expiration and retention."""
    
    def test_calculate_expires_at_default_24_months(self):
        """calculate_expires_at should compute correct expiration."""
        from cashu.core.proof_of_liabilities import calculate_expires_at
        
        expires = calculate_expires_at("2024-01-15", 24)
        assert expires is not None
        
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(expires, tz=timezone.utc)
        assert dt.year == 2026
        assert dt.month == 1
        assert dt.day == 15
    
    def test_calculate_expires_at_disabled(self):
        """calculate_expires_at returns None when retention is 0."""
        from cashu.core.proof_of_liabilities import calculate_expires_at
        
        assert calculate_expires_at("2024-01-15", 0) is None
        assert calculate_expires_at("2024-01-15", -1) is None
    
    def test_calculate_expires_at_handles_month_overflow(self):
        """calculate_expires_at handles months that overflow into next year."""
        from cashu.core.proof_of_liabilities import calculate_expires_at
        
        expires = calculate_expires_at("2024-10-15", 6)
        assert expires is not None
        
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(expires, tz=timezone.utc)
        assert dt.year == 2025
        assert dt.month == 4
    
    def test_calculate_expires_at_handles_day_overflow(self):
        """calculate_expires_at handles day overflow (31st -> 28th in Feb)."""
        from cashu.core.proof_of_liabilities import calculate_expires_at
        
        expires = calculate_expires_at("2024-01-31", 1)
        assert expires is not None
        
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(expires, tz=timezone.utc)
        assert dt.year == 2024
        assert dt.month == 2
        assert dt.day == 29
    
    def test_epoch_report_is_expired(self):
        """EpochReport.is_expired should return correct status."""
        past = int(time.time()) - 1000
        future = int(time.time()) + 1000
        
        expired_report = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a" * 64, mint_root_amount=100, burn_root_hash="b" * 64, burn_root_amount=10,
            report_timestamp=1704067200, report_hash="h" * 64, report_signature="",
            expires_at=past,
        )
        assert expired_report.is_expired()
        
        active_report = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a" * 64, mint_root_amount=100, burn_root_hash="b" * 64, burn_root_amount=10,
            report_timestamp=1704067200, report_hash="h" * 64, report_signature="",
            expires_at=future,
        )
        assert not active_report.is_expired()
        
        no_expiry_report = EpochReport(
            keyset_id="t", epoch_date="2024-01-01", epoch_start=1704067200, epoch_end=1704153599,
            total_minted=100, total_burned=10, outstanding_balance=90,
            mint_root_hash="a" * 64, mint_root_amount=100, burn_root_hash="b" * 64, burn_root_amount=10,
            report_timestamp=1704067200, report_hash="h" * 64, report_signature="",
            expires_at=None,
        )
        assert not no_expiry_report.is_expired()


class TestConfigurableTreeSize:
    """Tests for configurable Merkle tree size."""
    
    def test_custom_tree_size(self):
        """MerkleSumTree should accept custom fixed_tree_size."""
        tokens = [("a", 100), ("b", 200)]
        
        tree_default = MerkleSumTree(tokens, epoch_date="2024-01-01", use_fixed_padding=False)
        assert tree_default.padded_size == 2
        
        tree_custom = MerkleSumTree(
            tokens, epoch_date="2024-01-01", use_fixed_padding=True, fixed_tree_size=16
        )
        assert tree_custom.padded_size == 16
        
        for token, _ in tokens:
            proof = tree_custom.get_proof(token)
            assert proof is not None
            assert proof.verify()
    
    def test_tree_size_from_exponent(self):
        """Tree size should be 2^exponent."""
        tokens = [("a", 100)]
        
        tree = MerkleSumTree(
            tokens, epoch_date="2024-01-01", use_fixed_padding=True, fixed_tree_size=2**3
        )
        assert tree.padded_size == 8