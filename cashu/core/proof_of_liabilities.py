import hashlib
import json
import random
import calendar
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel

from cashu.core.settings import settings


def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()


class MerkleSumNode:
    __slots__ = ('hash', 'amount')

    def __init__(self, hash_value: str, amount: int):
        self.hash = hash_value
        self.amount = amount

    @staticmethod
    def leaf(value: str, amount: int) -> "MerkleSumNode":
        combined = f"{value}:{amount}"
        return MerkleSumNode(_sha256(combined), amount)

    @staticmethod
    def empty() -> "MerkleSumNode":
        return MerkleSumNode(_sha256("EMPTY:0"), 0)

    @staticmethod
    def combine(left: "MerkleSumNode", right: "MerkleSumNode") -> "MerkleSumNode":
        total_amount = left.amount + right.amount
        combined = f"{left.hash}:{right.hash}:{total_amount}"
        return MerkleSumNode(_sha256(combined), total_amount)


class MerkleSumProof(BaseModel):
    leaf_value: str
    leaf_amount: int
    siblings: List[Tuple[str, int, str]]
    root_hash: str
    root_amount: int
    
    def verify(self) -> bool:
        """Verify the proof is valid."""
        return MerkleSumTree.verify_proof(
            self.leaf_value,
            self.leaf_amount,
            self.siblings,
            self.root_hash,
            self.root_amount,
        )


class MerkleSumTree:
    def __init__(
        self,
        leaves: List[Tuple[str, int]],
        epoch_date: str,
        use_fixed_padding: bool = False,
        fixed_tree_size: Optional[int] = None,
    ):
        self.original_leaves = sorted(leaves, key=lambda x: x[0])
        self.use_fixed_padding = use_fixed_padding
        self.seed_value = _sha256(f"{self.original_leaves}:{epoch_date}")
        
        if use_fixed_padding:
            self.padded_size = fixed_tree_size if fixed_tree_size else 2 ** settings.pol_tree_size_exponent
        else:
            n = max(len(self.original_leaves), 1)
            self.padded_size = 1 << (n - 1).bit_length()
        
        self.tree, self.leaf_indices = self._build_tree()

    def _build_tree(self) -> Tuple[List[List[MerkleSumNode]], Dict[str, int]]:
        # Create leaf nodes with their original values for tracking
        leaf_data = [
            (value, amount, MerkleSumNode.leaf(value, amount))
            for value, amount in self.original_leaves
        ]
        
        # Add padding
        padding_count = self.padded_size - len(leaf_data)
        leaf_data.extend([("__EMPTY__", 0, MerkleSumNode.empty()) for _ in range(padding_count)])
        
        # Shuffle deterministically
        rng = random.Random(self.seed_value)
        rng.shuffle(leaf_data)
        
        # Build post-shuffle index mapping value -> shuffled position
        leaf_indices = {data[0]: i for i, data in enumerate(leaf_data) if data[0] != "__EMPTY__"}
        
        # Extract just the nodes for tree building
        leaf_nodes = [data[2] for data in leaf_data]
        
        tree = [leaf_nodes]
        level = leaf_nodes
        
        while len(level) > 1:
            next_level = []
            for i in range(0, len(level), 2):
                left = level[i]
                right = level[i + 1] if i + 1 < len(level) else MerkleSumNode.empty()
                next_level.append(MerkleSumNode.combine(left, right))
            tree.append(next_level)
            level = next_level
        
        return tree, leaf_indices

    @property
    def root(self) -> MerkleSumNode:
        return self.tree[-1][0] if self.tree else MerkleSumNode.empty()
    
    @property
    def root_hash(self) -> str:
        return self.root.hash
    
    @property
    def total_amount(self) -> int:
        return self.root.amount
    
    def get_proof(self, value: str) -> Optional[MerkleSumProof]:
        if value not in self.leaf_indices:
            return None
        
        idx = self.leaf_indices[value]
        # Get amount from the actual leaf node in the tree
        amount = self.tree[0][idx].amount
        siblings = []
        
        for level in self.tree[:-1]:
            is_right = idx % 2 == 1
            sibling_idx = idx - 1 if is_right else idx + 1
            
            if sibling_idx < len(level):
                sibling = level[sibling_idx]
                pos = "L" if is_right else "R"
                siblings.append((sibling.hash, sibling.amount, pos))
            
            idx //= 2
        
        return MerkleSumProof(
            leaf_value=value,
            leaf_amount=amount,
            siblings=siblings,
            root_hash=self.root_hash,
            root_amount=self.total_amount,
        )
    
    @staticmethod
    def verify_proof(
        value: str,
        amount: int,
        siblings: List[Tuple[str, int, str]],
        root_hash: str,
        root_amount: int,
    ) -> bool:
        current = MerkleSumNode.leaf(value, amount)
        
        for sibling_hash, sibling_amount, pos in siblings:
            sibling = MerkleSumNode(sibling_hash, sibling_amount)
            if pos == "L":
                current = MerkleSumNode.combine(sibling, current)
            else:
                current = MerkleSumNode.combine(current, sibling)
        
        return current.hash == root_hash and current.amount == root_amount


class MintProof(BaseModel):
    keyset_id: str
    amount: int
    B_: str
    C_: str
    dleq_e: Optional[str] = None
    dleq_s: Optional[str] = None
    created: int


class BurnProof(BaseModel):
    keyset_id: str
    amount: int
    secret: str
    Y: str
    C: str
    witness: Optional[str] = None
    created: int


class EpochReport(BaseModel):
    keyset_id: str
    epoch_date: str
    epoch_start: int
    epoch_end: int
    previous_epoch_hash: Optional[str] = None
    cumulative_minted: int = 0
    cumulative_burned: int = 0
    total_minted: int
    total_burned: int
    outstanding_balance: int
    mint_root_hash: str
    mint_root_amount: int
    burn_root_hash: str
    burn_root_amount: int
    report_timestamp: int
    report_hash: str
    report_signature: str
    ots_proof: Optional[str] = None
    ots_confirmed: bool = False
    expires_at: Optional[int] = None
    
    model_config = {"extra": "allow"}

    def is_expired(self) -> bool:
        """Check if this epoch has expired based on expires_at timestamp."""
        if self.expires_at is None:
            return False
        return int(datetime.now(timezone.utc).timestamp()) > self.expires_at

    def compute_hash(self) -> str:
        data = {
            "keyset_id": self.keyset_id,
            "epoch_date": self.epoch_date,
            "epoch_start": self.epoch_start,
            "epoch_end": self.epoch_end,
            "previous_epoch_hash": self.previous_epoch_hash,
            "cumulative_minted": self.cumulative_minted,
            "cumulative_burned": self.cumulative_burned,
            "total_minted": self.total_minted,
            "total_burned": self.total_burned,
            "outstanding_balance": self.outstanding_balance,
            "mint_root_hash": self.mint_root_hash,
            "mint_root_amount": self.mint_root_amount,
            "burn_root_hash": self.burn_root_hash,
            "burn_root_amount": self.burn_root_amount,
        }
        return _sha256(json.dumps(data, sort_keys=True, separators=(',', ':')))

    def verify_report_signature(self, mint_pubkey: str) -> bool:
        """Verify the mint's signature on this epoch report.
        
        Args:
            mint_pubkey: Hex-encoded compressed public key of the mint.
        """
        if not self.report_signature or not self.report_hash:
            return False
        from coincurve import PublicKey as CoinPublicKey, PublicKeyXOnly
        report_hash_bytes = bytes.fromhex(self.report_hash)
        pubkey = CoinPublicKey(bytes.fromhex(mint_pubkey))
        xonly = PublicKeyXOnly(pubkey.format()[1:])
        try:
            return xonly.verify(bytes.fromhex(self.report_signature), report_hash_bytes)
        except Exception:
            return False

    def verify_chain_link(self, previous: Optional["EpochReport"]) -> bool:
        if previous is None:
            return self.previous_epoch_hash is None
        
        if self.previous_epoch_hash != previous.report_hash:
            return False
        
        expected_cumulative_minted = previous.cumulative_minted + self.total_minted
        expected_cumulative_burned = previous.cumulative_burned + self.total_burned
        
        return (
            self.cumulative_minted == expected_cumulative_minted
            and self.cumulative_burned == expected_cumulative_burned
        )


class PoLMerkleRoots(BaseModel):
    keyset_id: str
    epoch_date: str
    is_closed: bool
    previous_epoch_hash: Optional[str] = None
    mint_root_hash: str
    mint_root_amount: int
    burn_root_hash: str
    burn_root_amount: int
    outstanding_balance: int
    cumulative_minted: int = 0
    cumulative_burned: int = 0
    report_timestamp: int
    report_hash: Optional[str] = None
    report_signature: Optional[str] = None
    ots_proof: Optional[str] = None
    ots_confirmed: bool = False


class PoLHistory(BaseModel):
    keyset_id: str
    epochs: List[EpochReport]
    chain_valid: bool = True

    def verify_chain(self) -> bool:
        if not self.epochs:
            return True
        
        sorted_epochs = sorted(self.epochs, key=lambda e: e.epoch_date)
        
        if sorted_epochs[0].previous_epoch_hash is not None:
            return False
        
        for i in range(1, len(sorted_epochs)):
            if not sorted_epochs[i].verify_chain_link(sorted_epochs[i - 1]):
                return False
        
        return True


class ChainValidationResult(BaseModel):
    keyset_id: str
    is_valid: bool
    total_epochs: int
    first_epoch: Optional[str] = None
    last_epoch: Optional[str] = None
    cumulative_minted: int = 0
    cumulative_burned: int = 0
    current_balance: int = 0
    invalid_at_epoch: Optional[str] = None
    error_message: Optional[str] = None


class WalletPoLRecord(BaseModel):
    keyset_id: str
    B_: str
    C_: str
    amount: int
    dleq_e: Optional[str] = None
    dleq_s: Optional[str] = None
    dleq_r: Optional[str] = None
    secret: str
    Y: str
    created: int
    burned: bool = False
    burned_at: Optional[int] = None


class MintCommitment(BaseModel):
    """Signed commitment from mint promising to include a token in an epoch."""
    keyset_id: str
    B_: str
    amount: int
    epoch_date: str
    timestamp: int
    signature: str
    
    def compute_message(self) -> str:
        """Compute the message that was signed (deterministic, excludes timestamp)."""
        data = f"{self.keyset_id}:{self.B_}:{self.amount}:{self.epoch_date}"
        return data
    
    def compute_hash(self) -> str:
        """Compute hash of the commitment message."""
        return _sha256(self.compute_message())

    def verify_signature(self, mint_pubkey: str) -> bool:
        """Verify the mint's signature on this commitment.
        
        Args:
            mint_pubkey: Hex-encoded compressed public key of the mint.
        """
        from coincurve import PublicKey as CoinPublicKey, PublicKeyXOnly
        message_hash = hashlib.sha256(self.compute_message().encode()).digest()
        pubkey = CoinPublicKey(bytes.fromhex(mint_pubkey))
        xonly = PublicKeyXOnly(pubkey.format()[1:])
        try:
            return xonly.verify(bytes.fromhex(self.signature), message_hash)
        except Exception:
            return False


def get_epoch_date(timestamp: Optional[int] = None) -> str:
    """Get the epoch date (UTC calendar day) for a given timestamp."""
    if timestamp is None:
        dt = datetime.now(timezone.utc)
    else:
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def get_epoch_boundaries(date_str: str) -> Tuple[int, int]:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start = int(dt.timestamp())
    end = start + 86400 - 1
    return start, end


def calculate_expires_at(epoch_date: str, retention_months: int) -> Optional[int]:
    """
    Calculate the expiration timestamp for an epoch based on retention period.
    
    Args:
        epoch_date: The epoch date in YYYY-MM-DD format
        retention_months: Number of months to retain the epoch (0 = never expires)
    
    Returns:
        Unix timestamp when the epoch expires, or None if retention is disabled
    """
    if retention_months <= 0:
        return None
    
    dt = datetime.strptime(epoch_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    year = dt.year + (dt.month + retention_months - 1) // 12
    month = (dt.month + retention_months - 1) % 12 + 1
    max_day = calendar.monthrange(year, month)[1]
    day = min(dt.day, max_day)
    expires_dt = datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)
    return int(expires_dt.timestamp())
