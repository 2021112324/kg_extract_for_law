"""Offline regression of the real adapter; no model, telemetry or DB calls."""

import asyncio
import copy
from unittest.mock import AsyncMock, Mock

import pytest

from app.infrastructure.information_extraction import langextract_adapter as module
from app.infrastructure.information_extraction.method.base import LangextractConfig


@pytest.fixture
def adapter(monkeypatch):
    monkeypatch.setattr(module, "Langfuse", None)
    monkeypatch.setattr(module.time, "sleep", Mock())
    config = LangextractConfig(model_name="offline", api_key="configured-test-secret",
                              api_url="http://example.invalid/chat", config={"stream": False})
    return module.LangextractAdapter(max_retries=5, config=config)


def invoke(adapter, prompt="BASE_SCHEMA", text="SOURCE_TEXT", examples=None):
    return adapter.extract_list_of_dict(prompt, examples or ["EXAMPLE"], text)


def prompts(mock):
    return [call.kwargs["kwargs"]["prompt_description"] for call in mock.await_args_list]


def test_different_errors_replace_feedback(adapter, monkeypatch):
    model = AsyncMock(side_effect=[ValueError("ERR_A"), ValueError("ERR_B"), []])
    monkeypatch.setattr(module, "run_with_timeout_async", model)
    assert asyncio.run(invoke(adapter)) == [{"text": "", "extractions": []}]
    sent = prompts(model)
    assert sent[0] == "BASE_SCHEMA"
    assert "ERR_A" in sent[1]
    assert "ERR_B" in sent[2] and "ERR_A" not in sent[2]
    assert all(p.count("BASE_SCHEMA") == 1 for p in sent)
    assert [call.args[0] for call in module.time.sleep.call_args_list] == [30, 60]


@pytest.mark.parametrize("first", ["timeout", "ordinary"])
def test_timeout_and_ordinary_errors_replace_each_other(adapter, monkeypatch, first):
    failures = [module.TimeoutException("TIMEOUT_A"), ValueError("ERR_B"), module.TimeoutException("TIMEOUT_C")]
    if first == "ordinary":
        failures = [ValueError("ERR_A"), module.TimeoutException("TIMEOUT_B"), ValueError("ERR_C")]
    model = AsyncMock(side_effect=failures + [[]])
    monkeypatch.setattr(module, "run_with_timeout_async", model)
    asyncio.run(invoke(adapter))
    sent = prompts(model)
    if first == "timeout":
        assert "上次问答超时" in sent[1]
        assert "ERR_B" in sent[2] and "上次问答超时" not in sent[2]
        assert "上次问答超时" in sent[3] and "ERR_B" not in sent[3]
    else:
        assert "ERR_A" in sent[1]
        assert "上次问答超时" in sent[2] and "ERR_A" not in sent[2]
        assert "ERR_C" in sent[3] and "上次问答超时" not in sent[3] and "ERR_A" not in sent[3]


@pytest.mark.parametrize("attempts", [1, 5])
def test_exhaustion_preserves_budget_and_final_cause(adapter, monkeypatch, attempts):
    adapter.max_retries = attempts
    errors = [ValueError(f"ERROR_{i}") for i in range(attempts)]
    model = AsyncMock(side_effect=errors)
    monkeypatch.setattr(module, "run_with_timeout_async", model)
    with pytest.raises(Exception) as caught:
        asyncio.run(invoke(adapter))
    assert model.await_count == attempts
    assert caught.value.__cause__ is errors[-1]
    assert f"ERROR_{attempts - 1}" in str(caught.value)
    assert module.time.sleep.call_count == attempts - 1


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("as_iterable", [False, True])
def test_first_success_preserves_inputs_options_and_result_shape(adapter, monkeypatch, stream, as_iterable):
    adapter.default_config.config["stream"] = stream
    original = copy.deepcopy(adapter.default_config.model_dump())
    document = module.data.AnnotatedDocument(text="SOURCE_TEXT", extractions=[])
    response = iter([document]) if as_iterable else document
    model = AsyncMock(return_value=response)
    monkeypatch.setattr(module, "run_with_timeout_async", model)
    examples = [{"marker": ["unchanged"]}]
    convert = Mock(return_value=[{"text": "SOURCE_TEXT", "extractions": []}])
    monkeypatch.setattr(adapter, "convert_document_to_dict_with_text", convert)
    assert asyncio.run(invoke(adapter, examples=examples)) == convert.return_value
    kwargs = model.await_args.kwargs["kwargs"]
    assert kwargs["prompt_description"] == "BASE_SCHEMA"
    assert kwargs["text_or_documents"] == "SOURCE_TEXT"
    assert kwargs["examples"] is examples and examples == [{"marker": ["unchanged"]}]
    assert kwargs["language_model_params"]["stream"] is stream
    assert kwargs["api_key"] == original["api_key"]
    convert.assert_called_once_with([document])
    module.time.sleep.assert_not_called()


def test_feedback_is_bounded_and_redacted_without_changing_base(adapter, monkeypatch):
    error = ValueError("ERR_PRIVATE configured-test-secret Bearer BEARER_SECRET "
                       "password=PASSWORD_SECRET api_key=OTHER_SECRET "
                       "https://name:URL_SECRET@example.invalid/api\nsecond line\x00" + "x" * 4000 +
                       "\nTraceback (most recent call last):\nSTACK_BODY")
    model = AsyncMock(side_effect=[error, []])
    monkeypatch.setattr(module, "run_with_timeout_async", model)
    base = "ORIGINAL_SCHEMA\nORIGINAL_INSTRUCTIONS"
    asyncio.run(invoke(adapter, prompt=base))
    sent = prompts(model)
    assert sent[0] == base and sent[1].endswith(base)
    for secret in ("configured-test-secret", "BEARER_SECRET", "PASSWORD_SECRET", "OTHER_SECRET", "URL_SECRET", "STACK_BODY"):
        assert secret not in sent[1]
    assert len(sent[1]) - len(base) < 1300
    assert "\x00" not in sent[1]
    assert "ValueError" in sent[1] and "ERR_PRIVATE" in sent[1]


def test_concurrent_calls_do_not_share_feedback(adapter, monkeypatch):
    captured = {"ONE": [], "TWO": []}

    async def model(*_args, **kwargs):
        request = kwargs["kwargs"]
        name = request["text_or_documents"]
        captured[name].append(request["prompt_description"])
        await asyncio.sleep(0)
        if len(captured[name]) == 1:
            raise ValueError("ERROR_" + name)
        return []

    monkeypatch.setattr(module, "run_with_timeout_async", model)

    async def run():
        return await asyncio.gather(invoke(adapter, prompt="BASE_ONE", text="ONE"),
                                    invoke(adapter, prompt="BASE_TWO", text="TWO"))

    assert asyncio.run(run()) == [[{"text": "", "extractions": []}]] * 2
    for name, other in (("ONE", "TWO"), ("TWO", "ONE")):
        assert "ERROR_" + name in captured[name][1]
        assert "ERROR_" + other not in captured[name][1]
        assert "BASE_" + other not in captured[name][1]


def test_rate_limit_wait_is_unchanged(adapter, monkeypatch):
    model = AsyncMock(side_effect=[ValueError("429 rate limit"), []])
    monkeypatch.setattr(module, "run_with_timeout_async", model)
    asyncio.run(invoke(adapter))
    assert [call.args[0] for call in module.time.sleep.call_args_list] == [30, 5]


@pytest.mark.parametrize("recover", [True, False])
def test_zh_law_counts_only_final_clause_failure(adapter, monkeypatch, recover):
    from app.infrastructure.information_extraction.base import Entity
    from app.infrastructure.information_extraction.v2.zh_law import ClauseExtractor
    from app.infrastructure.information_extraction.v2.zh_law.clause_extract import ClauseCache
    from app.infrastructure.information_extraction.v2.zh_law.test.test_zh_law_v2 import config

    outcomes = [ValueError("FIRST_ERROR"), ValueError("LATEST_ERROR"), []] if recover else [ValueError("FINAL_ERROR")] * 5
    model = AsyncMock(side_effect=outcomes)
    monkeypatch.setattr(module, "run_with_timeout_async", model)
    graph = {"entities": [Entity(name="第一条", entity_type="法条", properties={})], "relations": []}
    monkeypatch.setattr(adapter, "convert_document_list_to_graph_dict", Mock(return_value=graph))
    extractor = ClauseExtractor(config=config(), extractor=adapter)
    call = extractor.kg_extract_from_clause("测试法规.txt", ClauseCache(),
        {"条款编号": "第一条", "条款内容": "企业应当依法履行义务。"})
    if recover:
        assert asyncio.run(call)["node_type"] == "法条"
        assert extractor.result_stats.error == 0
        assert model.await_count == 3
    else:
        with pytest.raises(ValueError):
            asyncio.run(call)
        assert extractor.result_stats.extraction_error == 1
        assert extractor.result_stats.error == 1
        assert "测试法规.txt" in extractor.result_stats.error_msg and "第一条" in extractor.result_stats.error_msg
        assert model.await_count == 5
