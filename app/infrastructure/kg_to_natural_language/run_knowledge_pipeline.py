"""一键运行：四类图谱 → 单表知识记录 → 自包含可视化看板（可选入库 MySQL 8）。

用法（在项目根目录执行）：
    python app/infrastructure/kg_to_natural_language/run_knowledge_pipeline.py

默认流程（离线，不连接 MySQL）：
    1. 只读连接项目 Neo4j（复用 .env 中的 NEO4J_* 配置），读取四个节点标签
       「法律法规条款 / 国家标准 / 合规指引 / 行政监管规则」的图谱快照。
    2. 调用 storage.pipeline.prepare_knowledge，把图谱按各自转换器转成知识条目，
       合并去重后冻结为 records.jsonl（单表 t_natural_knowledge_entry 的行原型）。
    3. 从 records.jsonl 生成一个自包含 HTML 看板 dashboard.html（可检索、可筛选）。

如需真正写入 MySQL 8（单表存储的核心动作），把 IMPORT_TO_MYSQL 置为 True，
并先启动 MySQL 8 实例，同时通过环境变量 KNOWLEDGE_STORAGE_DB_URL 指定连接串。
"""

import json
import logging
import sys
from collections import Counter
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# 配置区
# ---------------------------------------------------------------------------
# 四个业务标签及其转换器：法律法规条款、行政监管规则都走 chinese_law。
SOURCES = [
    ("法律法规条款", "chinese_law"),
    ("国家标准", "national_standard"),
    ("合规指引", "guide"),
    ("行政监管规则", "chinese_law"),
]
DATASET_VERSION = "knowledge_20261103_v1"
OUTPUT_ROOT = SCRIPT_DIR / "output" / "knowledge_pipeline"
BATCH_SIZE = 200

# 入库开关：置 True 时，先启动 MySQL 8 并通过 KNOWLEDGE_STORAGE_DB_URL 指定连接串。
IMPORT_TO_MYSQL = False

# 行业归一只使用明确配置，未配置不会自动填“其他”。
INDUSTRY_MAPPING = {}
FILENAME_INDUSTRIES = {}
FILE_INDICATOR_PROPERTIES = {}
FORMAL_INDICATOR_PROPERTY = "三级指标映射列表"
INDICATOR_SYSTEM_VERSION = None

logger = logging.getLogger(__name__)


def build_config():
    from app.infrastructure.kg_to_natural_language.storage.config import SourceConfig, StorageConfig

    config = StorageConfig(
        dataset_version=DATASET_VERSION,
        sources=[SourceConfig(
            source_type="neo4j", converter=converter, category=label,
            graph_label=label, graph_source="project-neo4j",
        ) for label, converter in SOURCES],
        output_root=str(OUTPUT_ROOT),
        batch_size=BATCH_SIZE,
        industry_mapping=INDUSTRY_MAPPING,
        filename_industries=FILENAME_INDUSTRIES,
        file_indicator_properties=FILE_INDICATOR_PROPERTIES,
        formal_indicator_property=FORMAL_INDICATOR_PROPERTY,
        indicator_system_version=INDICATOR_SYSTEM_VERSION,
    )
    config.validate()
    return config


def load_records(record_directory):
    path = Path(record_directory) / "records.jsonl"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return rows


def build_summary(rows):
    files = set()
    for row in rows:
        for source in row.get("sources") or []:
            for name in source.get("filenames") or []:
                if name:
                    files.add(name)
            if source.get("filename"):
                files.add(source["filename"])
    return {
        "total": len(rows),
        "with_indicators": sum(1 for r in rows if r.get("indicators")),
        "by_category": dict(Counter(r.get("knowledge_category", "未分类") for r in rows)),
        "by_type": dict(Counter(r.get("knowledge_type", "未分类") for r in rows)),
        "by_risk": dict(Counter(rt for r in rows for rt in (r.get("risk_types") or []))),
        "by_industry": dict(Counter(ind for r in rows for ind in (r.get("economic_industries") or []))),
        "source_files": len(files),
    }


def build_table_rows(rows):
    table = []
    for r in rows:
        indicators = []
        for it in r.get("indicators") or []:
            num = it.get("indicator_number", "")
            name = it.get("indicator_name", "")
            indicators.append({"n": num, "name": name, "text": f"{num} {name}".strip()})
        filenames = []
        for source in r.get("sources") or []:
            filenames.extend(source.get("filenames") or [])
            if source.get("filename"):
                filenames.append(source["filename"])
        table.append({
            "category": r.get("knowledge_category", ""),
            "type": r.get("knowledge_type", ""),
            "content": r.get("content", ""),
            "validity": r.get("validity_status") or "",
            "risk": "、".join(r.get("risk_types") or []),
            "industry": "、".join(r.get("economic_industries") or []),
            "indicators": indicators,
            "indicator_text": "、".join(i["text"] for i in indicators),
            "files": "、".join(dict.fromkeys(f for f in filenames if f)),
        })
    return table


def render_dashboard(summary, table_rows, dashboard_path):
    summary_json = json.dumps(summary, ensure_ascii=False)
    rows_json = json.dumps(table_rows, ensure_ascii=False)
    html = _DASHBOARD_TEMPLATE.replace("$SUMMARY_JSON$", summary_json).replace("$ROWS_JSON$", rows_json)
    dashboard_path.write_text(html, encoding="utf-8")
    return dashboard_path


def run():
    from app.infrastructure.kg_to_natural_language.storage.pipeline import prepare_knowledge
    from app.infrastructure.kg_to_natural_language.storage.records import json_default

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    config = build_config()
    logger.info("[1/2] 只读读取四类 Neo4j 图谱并生成知识条目（离线，不写 MySQL）")
    summary = prepare_knowledge(config)
    if summary.get("status") not in {"prepared", "empty"}:
        raise RuntimeError("未生成可入库知识条目，请查看运行目录中的 summary.json / events.jsonl")

    record_directory = summary["record_directory"]
    logger.info("知识条目冻结目录：%s", record_directory)
    logger.info("输入节点 %s 个、关系 %s 条；生成条目 %s，合并去重后 %s 条",
                summary.get("input_nodes"), summary.get("input_relationships"),
                summary.get("generated_entries"), summary.get("records"))

    dashboard_path = OUTPUT_ROOT / "dashboard.html"
    if summary.get("records"):
        rows = load_records(record_directory)
        overview = build_summary(rows)
        table_rows = build_table_rows(rows)
        render_dashboard(overview, table_rows, dashboard_path)
        logger.info("[2/2] 已生成可视化看板：%s", dashboard_path)
    else:
        logger.warning("本次没有可入库条目，跳过看板生成；请检查四个 Neo4j 标签是否有数据")

    if IMPORT_TO_MYSQL:
        _import_to_mysql(record_directory)

    result = {
        "status": summary.get("status"),
        "dataset_version": DATASET_VERSION,
        "records": summary.get("records"),
        "record_directory": record_directory,
        "dashboard": str(dashboard_path),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=json_default))
    return 0


def _import_to_mysql(record_directory):
    import os
    from app.infrastructure.kg_to_natural_language.storage.pipeline import persist_knowledge
    from app.infrastructure.kg_to_natural_language.storage.repository import KnowledgeRepository
    from sqlalchemy import create_engine

    url = os.environ.get("KNOWLEDGE_STORAGE_DB_URL")
    if not url:
        raise ValueError("IMPORT_TO_MYSQL=True 需要先设置环境变量 KNOWLEDGE_STORAGE_DB_URL")
    engine = create_engine(url, pool_pre_ping=True, hide_parameters=True)
    try:
        result = persist_knowledge(record_directory, repository=KnowledgeRepository(engine),
                                   initialize_schema=True)
        logger.info("入库结果：%s", result.get("status"))
    finally:
        engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        raise SystemExit(run())
    except Exception as exc:
        from app.infrastructure.kg_to_natural_language.storage.local_records import redact
        logger.error("执行失败（%s）：%s", type(exc).__name__, redact(str(exc)))
        raise SystemExit(2)


# ---------------------------------------------------------------------------
# 自包含看板模板（无外部依赖，纯 CSS/JS）
# ---------------------------------------------------------------------------
_DASHBOARD_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>知识库 · 单表知识可视化</title>
<style>
:root{
  --bg:#f6f7f9; --card:#ffffff; --line:#e5e7eb; --ink:#1f2937; --muted:#6b7280;
  --accent:#2563eb; --accent2:#0ea5e9;
}
*{box-sizing:border-box}
body{margin:0;font-family:-apple-system,"Segoe UI","Microsoft YaHei",Roboto,Helvetica,Arial,sans-serif;
     background:var(--bg);color:var(--ink);line-height:1.55}
.wrap{max-width:1180px;margin:0 auto;padding:24px 20px 60px}
h1{font-size:22px;margin:0 0 4px}
.sub{color:var(--muted);font-size:13px;margin:0 0 20px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:20px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.card .k{color:var(--muted);font-size:12px}
.card .v{font-size:26px;font-weight:700;margin-top:4px}
.panel{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:18px;margin-bottom:18px}
.panel h2{font-size:15px;margin:0 0 14px}
.bars{display:flex;flex-direction:column;gap:8px}
.bar-row{display:grid;grid-template-columns:150px 1fr 60px;align-items:center;gap:10px;font-size:13px}
.bar-label{color:var(--muted);text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bar-track{background:#eef1f5;border-radius:6px;height:16px;overflow:hidden}
.bar-fill{height:100%;border-radius:6px;background:linear-gradient(90deg,var(--accent),var(--accent2))}
.bar-num{color:var(--ink);font-weight:600}
.filters{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:14px}
.filters input[type=text]{flex:1;min-width:220px}
.filters input,.filters select{padding:8px 10px;border:1px solid var(--line);border-radius:8px;font-size:13px;background:#fff}
.filters label{display:flex;align-items:center;gap:6px;font-size:13px;color:var(--muted)}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:9px 10px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-weight:600;background:#fafbfc;position:sticky;top:0}
td.content{max-width:520px}
.badge{display:inline-block;padding:1px 8px;border-radius:999px;font-size:11px;background:#eef4ff;color:var(--accent);margin:1px 2px 1px 0}
.badge.gray{background:#f0f1f3;color:#6b7280}
.badge.green{background:#e7f7ef;color:#059669}
.ind{color:#059669;font-size:12px}
.pager{display:flex;align-items:center;justify-content:flex-end;gap:12px;margin-top:12px;font-size:13px;color:var(--muted)}
.pager button{padding:6px 12px;border:1px solid var(--line);border-radius:8px;background:#fff;cursor:pointer}
.pager button:disabled{opacity:.4;cursor:default}
.muted{color:var(--muted)}
</style>
</head>
<body>
<div class="wrap">
  <h1>知识库 · 单表知识可视化</h1>
  <p class="sub">数据来自 <code>records.jsonl</code>（四类标签：法律法规条款 / 国家标准 / 合规指引 / 行政监管规则）</p>

  <div class="cards" id="cards"></div>

  <div class="panel">
    <h2>分类分布</h2><div class="bars" id="chart-category"></div>
  </div>
  <div class="panel">
    <h2>知识类型分布</h2><div class="bars" id="chart-type"></div>
  </div>
  <div class="panel">
    <h2>风险类型分布</h2><div class="bars" id="chart-risk"></div>
  </div>
  <div class="panel">
    <h2>经济行业分布</h2><div class="bars" id="chart-industry"></div>
  </div>

  <div class="panel">
    <h2>明细检索</h2>
    <div class="filters">
      <input type="text" id="q" placeholder="关键词搜索内容…">
      <select id="f-cat"><option value="">全部分类</option></select>
      <select id="f-type"><option value="">全部知识类型</option></select>
      <label><input type="checkbox" id="f-ind"> 仅看含正式指标</label>
    </div>
    <div id="table-wrap"></div>
    <div class="pager">
      <span id="page-info"></span>
      <button id="prev">上一页</button>
      <button id="next">下一页</button>
    </div>
  </div>
</div>

<script>
const SUMMARY = $SUMMARY_JSON$;
const ROWS = $ROWS_JSON$;

function el(id){return document.getElementById(id)}
function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]))}

function bars(nodeId, data){
  const total = Object.values(data).reduce((a,b)=>a+b,0)||1;
  const sorted = Object.entries(data).sort((a,b)=>b[1]-a[1]);
  el(nodeId).innerHTML = sorted.map(([k,v])=>
    `<div class="bar-row"><div class="bar-label" title="${esc(k)}">${esc(k)}</div>
     <div class="bar-track"><div class="bar-fill" style="width:${(v/total*100).toFixed(1)}%"></div></div>
     <div class="bar-num">${v}</div></div>`).join("") || '<div class="muted">无数据</div>';
}

// 统计卡片
(function(){
  const s = SUMMARY;
  const cats = Object.entries(s.by_category).sort((a,b)=>b[1]-a[1]);
  const cards = [
    {k:"知识条目总数", v:s.total},
    {k:"含正式指标", v:s.with_indicators},
    {k:"来源文件数", v:s.source_files},
  ];
  cats.forEach(([c,v])=>cards.push({k:"分类·"+c, v}));
  el("cards").innerHTML = cards.map(c=>`<div class="card"><div class="k">${esc(c.k)}</div><div class="v">${c.v}</div></div>`).join("");
  bars("chart-category", s.by_category);
  bars("chart-type", s.by_type);
  bars("chart-risk", s.by_risk);
  bars("chart-industry", s.by_industry);
})();

// 筛选下拉
(function(){
  const catSet=[...new Set(ROWS.map(r=>r.category))].sort();
  const typeSet=[...new Set(ROWS.map(r=>r.type))].sort();
  el("f-cat").innerHTML += catSet.map(c=>`<option>${esc(c)}</option>`).join("");
  el("f-type").innerHTML += typeSet.map(c=>`<option>${esc(c)}</option>`).join("");
})();

let page=1; const PAGE=50;
function filtered(){
  const q=el("q").value.trim().toLowerCase();
  const cat=el("f-cat").value, type=el("f-type").value, onlyInd=el("f-ind").checked;
  return ROWS.filter(r=>{
    if(cat && r.category!==cat) return false;
    if(type && r.type!==type) return false;
    if(onlyInd && !r.indicators.length) return false;
    if(q && !r.content.toLowerCase().includes(q)) return false;
    return true;
  });
}
function render(){
  const list=filtered();
  const total=list.length, pages=Math.max(1,Math.ceil(total/PAGE));
  if(page>pages) page=pages;
  const slice=list.slice((page-1)*PAGE, page*PAGE);
  const head=`<table><thead><tr><th>分类</th><th>知识类型</th><th>内容</th><th>风险类型</th><th>经济行业</th><th>正式指标</th><th>来源文件</th></tr></thead><tbody>`;
  const body=slice.map(r=>{
    const inds=r.indicators.length
      ? r.indicators.map(i=>`<span class="badge green" title="${esc(i.text)}">${esc(i.n)}</span>`).join("")
      : `<span class="badge gray">无</span>`;
    return `<tr>
      <td><span class="badge">${esc(r.category)}</span></td>
      <td>${esc(r.type)}</td>
      <td class="content">${esc(r.content)}</td>
      <td>${esc(r.risk)||'<span class="muted">—</span>'}</td>
      <td>${esc(r.industry)||'<span class="muted">—</span>'}</td>
      <td class="ind">${inds}</td>
      <td>${esc(r.files)||'<span class="muted">—</span>'}</td>
    </tr>`;
  }).join("");
  el("table-wrap").innerHTML=head+body+`</tbody></table>`;
  el("page-info").textContent=`共 ${total} 条 · 第 ${page}/${pages} 页`;
  el("prev").disabled = page<=1;
  el("next").disabled = page>=pages;
}
el("q").addEventListener("input",()=>{page=1;render()});
el("f-cat").addEventListener("change",()=>{page=1;render()});
el("f-type").addEventListener("change",()=>{page=1;render()});
el("f-ind").addEventListener("change",()=>{page=1;render()});
el("prev").addEventListener("click",()=>{page--;render()});
el("next").addEventListener("click",()=>{page++;render()});
render();
</script>
</body>
</html>
"""
