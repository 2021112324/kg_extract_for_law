"""Conservative syntax-only repair with lossless token comparison."""

import ast
from contextvars import ContextVar
from importlib.metadata import PackageNotFoundError, version
import json
import re

POLICY = "syntax-only-token-preserving-v1"
_audit = ContextVar("risk_map_json_parse", default=None)


class OutputError(ValueError):
    pass


def dependency_version():
    try:
        return version("json-repair")
    except PackageNotFoundError:
        return "unavailable"


def reset_audit():
    _audit.set(None)


def parse_audit():
    return _audit.get()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise OutputError("duplicate_json_key:" + key[:80])
        result[key] = value
    return result


def _strict(text):
    def invalid(value):
        raise OutputError("nonfinite_json_value:" + value)
    return json.loads(text, object_pairs_hook=_unique, parse_constant=invalid)


def _tokens(text):
    # Decode scalar values with standard parsers. Only comma/colon changes may
    # disappear; brackets, value order, types and values must survive unchanged.
    decoder = json.JSONDecoder()
    tokens, stack = [], []
    i, closed = 0, False
    while i < len(text):
        char = text[i]
        if char.isspace():
            i += 1
            continue
        if closed:
            raise OutputError("multiple_or_trailing_json_content")
        if char in "{}[],:":
            if char in "{[":
                stack.append(char)
            elif char in "}]":
                if not stack or stack.pop() != {"}": "{", "]": "["}[char]:
                    raise OutputError("incomplete_or_unbalanced_json")
                closed = not stack
            if char not in ",:":
                tokens.append(("punctuation", char))
            i += 1
            continue
        if not stack:
            raise OutputError("expected_single_json_container")
        try:
            if char == "'":
                match = re.match(r"'(?:\\.|[^'\\])*'", text[i:])
                if not match:
                    raise OutputError("incomplete_json_string")
                value = ast.literal_eval(match.group())
                end = i + len(match.group())
            else:
                value, end = decoder.raw_decode(text, i)
        except (ValueError, SyntaxError) as exc:
            raise OutputError("unsafe_or_incomplete_json_value") from exc
        if isinstance(value, (dict, list)):
            raise OutputError("unsafe_json_token")
        tokens.append((type(value).__name__, value))
        i = end
    if stack or not closed:
        raise OutputError("incomplete_or_unbalanced_json")
    return tokens


def parse_output(value):
    if not isinstance(value, str):
        return value
    fenced = re.fullmatch(r"```(?:json)?\s*([\s\S]*?)\s*```", value.strip(), re.I)
    text = fenced.group(1) if fenced else value.strip()
    try:
        return _strict(text)
    except json.JSONDecodeError as exc:
        syntax_error = f"invalid_json:line={exc.lineno},column={exc.colno}"
    audit = {"attempted": True, "success": False, "original_response": value,
             "policy": POLICY, "syntax_error": syntax_error}
    _audit.set(audit)
    try:
        before = _tokens(text)
        from json_repair import repair_json
        repaired = repair_json(text, return_objects=False, ensure_ascii=False)
        if not isinstance(repaired, str):
            raise OutputError("repair_did_not_return_text")
        audit["repaired_response"] = repaired
        answer = _strict(repaired)
        if before != _tokens(repaired):
            raise OutputError("repair_changes_business_tokens")
    except (ImportError, ValueError, TypeError, RecursionError) as exc:
        audit["error"] = str(exc) if isinstance(exc, OutputError) else type(exc).__name__
        raise OutputError(syntax_error + ";repair_rejected:" + audit["error"]) from exc
    audit["success"] = True
    return answer
