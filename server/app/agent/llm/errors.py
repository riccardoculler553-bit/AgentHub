"""LLM-layer errors, kept separate from Tool errors (PDF §129).

LLMTransientError -> bounded retry, max_llm_retries=2, transient failures
only (429 / 5xx / timeout / connection, PDF §127).

LLMOutputError    -> the model answer could not be parsed into the decision
schema; re-request exactly once, then the run FAILS - never guess what the
model meant (PDF §128).
"""


class LLMError(Exception):
    """Base class for LLM-layer failures."""


class LLMTransientError(LLMError):
    """Transient provider failure; safe to retry a bounded number of times."""


class LLMOutputError(LLMError):
    """Structured output missing/invalid; one re-request, then fail."""
