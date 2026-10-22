"""Chinese clause locator normalization reused from the original evidence parser."""

from dataclasses import dataclass
import re

NUM = r"[0-9０-９零〇一二三四五六七八九十百千万两]+"
TOKEN = rf"第(?P<a>{NUM})(?:(?:条)?[—–－\-至到]第?(?P<z>{NUM}))?条(?:之(?P<i>{NUM}))?(?:第(?P<p>{NUM})款)?(?:第(?P<t>{NUM})项)?"
LOCATOR = re.compile(TOKEN)
HEAD = re.compile(rf"(?m)(?:^|(?<=[。；;]))[ \t]*(?:[-*][ \t]*)?(?P<loc>{TOKEN})(?=\s|[|｜:：、.．]|$)")


@dataclass(frozen=True)
class Locator:
    article: int
    inserted: int = 0
    paragraph: int | None = None
    item: int | None = None
    range_end: int | None = None
    raw: str = ""


def chinese_number(value):
    value = value.translate(str.maketrans("０１２３４５６７８９〇两", "0123456789零二"))
    if value.isdigit():
        return int(value)
    digits = dict(zip("零一二三四五六七八九", range(10)))
    total = section = number = 0
    for char in value:
        if char in digits:
            number = digits[char]
        elif char == "万":
            total += (section + number) * 10000
            section = number = 0
        elif char in "十百千":
            section += (number or 1) * {"十": 10, "百": 100, "千": 1000}[char]
            number = 0
        else:
            raise ValueError("非法中文数字")
    return total + section + number


def parse_locator(text):
    match = LOCATOR.fullmatch(text.strip())
    if not match:
        return None
    parts = {k: chinese_number(v) if v is not None else None for k, v in match.groupdict().items()}
    if parts["a"] <= 0:
        return None
    return Locator(parts["a"], parts["i"] or 0, parts["p"], parts["t"], parts["z"], match.group())
