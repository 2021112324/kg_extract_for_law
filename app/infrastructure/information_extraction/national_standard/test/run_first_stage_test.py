import json
import sys
from pathlib import Path


# =========================
# 可修改参数
# =========================

# 输入可以是：
# 1. 包含多份国家标准子目录的根目录；
# 2. 单份国家标准目录；
# 3. 单个 full.md / full_fixed.md / reviewed.md / txt 文件。
INPUT_DIR = Path(
    r"D:\CogmAIT\8.1项目\8.1数据\爬取的数据\风险法规\第二版\分类\中文抽取所用数据\国家标准"
)

# 输出目录。每份标准会输出一个 JSON，另外会生成 summary.json。
OUTPUT_DIR = (
    Path(__file__).resolve().parents[1]
    / "result"
    / "first_stage_test"
)

# 是否上传图片到 MinIO。
# 仅测试结构解析时建议 False；需要验证图片 MinIO 链接时再改为 True。
UPLOAD_IMAGES = False


# =========================
# 脚本逻辑
# =========================

PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.infrastructure.information_extraction.national_standard.national_standard_extract import (  # noqa: E402
    extract_national_standard_batch,
)


def main() -> None:
    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"输入路径不存在: {INPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = extract_national_standard_batch(
        input_root_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        upload_images=UPLOAD_IMAGES,
    )

    summary_path = OUTPUT_DIR / "summary.json"
    print("国家标准一阶段测试完成")
    print("input_dir:", INPUT_DIR)
    print("output_dir:", OUTPUT_DIR)
    print("upload_images:", UPLOAD_IMAGES)
    print("summary_path:", summary_path)
    print(
        "summary:",
        json.dumps(
            {
                "total": summary.get("total"),
                "success": summary.get("success"),
                "failed": summary.get("failed"),
                "validation_failed": summary.get("validation_failed"),
            },
            ensure_ascii=False,
        ),
    )

    failed_items = [
        item for item in summary.get("items", [])
        if item.get("status") != "success"
    ]
    if failed_items:
        print("失败或校验未通过的数据：")
        for item in failed_items:
            print(
                "-",
                item.get("name"),
                "| status:",
                item.get("status"),
                "| error:",
                item.get("error", ""),
            )


if __name__ == "__main__":
    main()
