__all__ = [
    "NormalizedPolicyRecord",
    "normalize_policy_record",
    "run_normalization",
]


def __getattr__(name):
    if name in __all__:
        from .normalize import (
            NormalizedPolicyRecord,
            normalize_policy_record,
            run_normalization,
        )

        namespace = {
            "NormalizedPolicyRecord": NormalizedPolicyRecord,
            "normalize_policy_record": normalize_policy_record,
            "run_normalization": run_normalization,
        }
        return namespace[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
