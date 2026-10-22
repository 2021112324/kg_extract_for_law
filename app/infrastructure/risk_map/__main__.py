"""One entry point; legacy JSON file/batch and ranking commands are removed."""

import sys


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if args:
        print("旧JSON图谱/排名命令已移除。请修改risk_map/run_mapping.py配置后直接执行；图谱输入为Neo4j标签。")
        return 64
    from .run_mapping import main as run
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
