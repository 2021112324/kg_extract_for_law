"""Serializable, credential-redacted exception details without captured locals."""

import re
import traceback


def redact_diagnostics(value, secrets=()):
    if isinstance(value, str):
        for secret in sorted({s for s in secrets if isinstance(s, str) and s}, key=len, reverse=True):
            value = value.replace(secret, "[REDACTED]")
        value = re.sub(r"(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+:[^\s/@]+@", r"\1[REDACTED]@", value)
        return re.sub(r"(?i)(?:bearer\s+|(?:api[_-]?key|password|passwd|token)[\"']?\s*[:=]\s*[\"']?)[^\s\"',}]+",
                      "[REDACTED]", value)
    if isinstance(value, dict):
        return {k: ("[REDACTED]" if re.fullmatch(r"(?i)password|passwd|api[_-]?key|authorization|token", k)
                    else redact_diagnostics(v, secrets)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_diagnostics(v, secrets) for v in value]
    return value


def exception_details(exc, secrets=()):
    result = {"error_type": type(exc).__name__,
              "error_class": type(exc).__module__ + "." + type(exc).__qualname__,
              "error": str(exc),
              "traceback": "".join(traceback.TracebackException.from_exception(exc, capture_locals=False).format())}
    for name in ("code", "gql_status"):
        value = getattr(exc, name, None)
        if isinstance(value, str):
            result["neo4j_" + name] = value
    return redact_diagnostics(result, secrets)
