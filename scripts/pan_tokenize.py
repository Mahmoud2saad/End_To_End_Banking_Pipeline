"""
scripts/pan_tokenize.py

Deterministic PAN/card-number tokenization for the Banking Pipeline.

Design:
- HMAC-SHA256 keyed by PAN_TOKENIZATION_KEY (never hardcoded, never logged).
- Deterministic: the same PAN always tokenizes to the same token, so joins
  across tables (cards.csv <-> cards_data.csv <-> transactions) still work
  downstream without ever re-exposing the raw PAN.
- One-way: there is no detokenize() function in this module by design.
  A real reversible lookup (for a fraud investigation, say) is a *separate*,
  tightly access-controlled service — never something dbt, Grafana, or an
  analyst role can call. See docs/SECURITY_AND_GOVERNANCE.md section 1.
- Preserves the last 4 digits in the display form for operational use
  (e.g. "confirm the card ending in 4821 with the customer"), matching how
  real card processors mask PANs in agent-facing tools.

Usage (in the Kafka producer / stream_simulator, BEFORE anything is
written to a topic or a landing file):

    from scripts.pan_tokenize import tokenize_pan

    df["pan_token"] = df["PAN"].apply(tokenize_pan)
    df = df.drop(columns=["PAN"])   # raw PAN never proceeds past this point
"""
import hashlib
import hmac
import os
import re
from functools import lru_cache


class PanTokenizationError(Exception):
    pass


def _get_key() -> bytes:
    key = os.getenv("PAN_TOKENIZATION_KEY")
    if not key:
        raise PanTokenizationError(
            "PAN_TOKENIZATION_KEY is not set. Refusing to tokenize PANs "
            "without a key — this would either crash loudly (good) or, if "
            "someone 'fixes' this by falling back to a default key, "
            "silently produce insecure tokens (bad). Set the key via the "
            "secrets backend, not this fallback path."
        )
    return key.encode("utf-8")


def _normalize(raw_pan: str) -> str:
    """Strips whitespace/separators so '4111 1111 1111 1111' and
    '4111-1111-1111-1111' tokenize identically."""
    return re.sub(r"[\s\-]", "", str(raw_pan))


@lru_cache(maxsize=100_000)
def _hmac_digest(normalized_pan: str, key: bytes) -> str:
    return hmac.new(key, normalized_pan.encode("utf-8"), hashlib.sha256).hexdigest()


def tokenize_pan(raw_pan: str) -> str:
    """
    Returns a deterministic, irreversible token for a raw PAN.
    Format: TKN_<16 hex chars>  — fixed-length, joinable, not a valid PAN
    shape (so it can never be mistaken for a real card number downstream).
    """
    if raw_pan is None or str(raw_pan).strip() == "":
        return None

    normalized = _normalize(raw_pan)
    digest = _hmac_digest(normalized, _get_key())
    return f"TKN_{digest[:16]}"


def masked_display(raw_pan: str) -> str:
    """
    Returns a display-safe masked form preserving the last 4 digits, e.g.
    '****-****-****-4821'. Safe to show in an ops UI; NOT reversible to the
    full PAN, and NOT the same value as tokenize_pan()'s join key.
    """
    if raw_pan is None:
        return None
    normalized = _normalize(raw_pan)
    digits = re.sub(r"\D", "", normalized)
    if len(digits) < 4:
        return "****"
    return f"****-****-****-{digits[-4:]}"


if __name__ == "__main__":
    # Quick self-test — requires PAN_TOKENIZATION_KEY to be set in the env.
    os.environ.setdefault("PAN_TOKENIZATION_KEY", "local-dev-test-key-do-not-use-in-prod")
    sample = "4111 1111 1111 4821"
    print(f"raw:      {sample}")
    print(f"token:    {tokenize_pan(sample)}")
    print(f"masked:   {masked_display(sample)}")
    print(f"repeat:   {tokenize_pan(sample)} (should match token above — deterministic)")