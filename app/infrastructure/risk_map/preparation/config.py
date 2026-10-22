"""Authoritative risk roots and supported mapping regions."""

from pathlib import Path

RISK_CODES = {
    "企业关联方合规风险": "1", "产品法律风险": "2", "企业信用风险": "3",
    "劳动用工法律合规风险": "4", "企业国际化经营合规风险": "5", "供应链合规风险": "6",
}
REGIONS = frozenset({"中国", "美国", "欧盟及成员国", "国际组织"})
DEFAULT_TREE_PATH = Path(
    r"D:\CogmAIT\8.1项目\8.1数据\合规风险指标映射数据\v1_923\指标文件"
    r"\处理后的指标文件\三级指标映射\三级指标文件条款映射树.json")
