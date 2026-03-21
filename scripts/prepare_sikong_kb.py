from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw" / "sikong"
OUTPUT_ROOT = REPO_ROOT / "docs" / "ragflow" / "sikong-kb-pack"
MAIN_KB_DIR = OUTPUT_ROOT / "main-kb"
NORMALIZED_DIR = REPO_ROOT / "data" / "normalized"
CURATED_JSONL = NORMALIZED_DIR / "sikong_curated_ragflow.jsonl"
FULL_JSONL = NORMALIZED_DIR / "sikong_full_cleaned.jsonl"

QA_PATTERN = re.compile(
    r'\{\s*"input"\s*:\s*"(.*?)"\s*,\s*"output"\s*:\s*"(.*?)"\s*\}',
    re.S,
)
HTML_BREAK_RE = re.compile(r"<br\s*/?>", re.I)
HTML_TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class SourceRule:
    stem: str
    mode: str = "all"
    keywords: tuple[str, ...] = ()


@dataclass(frozen=True)
class ThemeRule:
    filename: str
    title: str
    description: str
    upload_priority: str
    sources: tuple[SourceRule, ...]


THEMES: tuple[ThemeRule, ...] = (
    ThemeRule(
        filename="01-建筑物理与热舒适基础.md",
        title="建筑物理与热舒适基础",
        description="适合支撑能耗解释、热环境说明、围护结构与气候影响问答。",
        upload_priority="最高",
        sources=(
            SourceRule("building-physics", "all"),
        ),
    ),
    ThemeRule(
        filename="02-建筑环境设计与公共建筑原则.md",
        title="建筑环境设计与公共建筑原则",
        description="适合支撑通风、采光、天窗、公共建筑环境设计与节能原则问答。",
        upload_priority="高",
        sources=(
            SourceRule(
                "architecture-introduction",
                "filter",
                (
                    "采光",
                    "日照",
                    "通风",
                    "换气",
                    "遮阳",
                    "天窗",
                    "屋面",
                    "窗",
                    "门窗",
                    "保温",
                    "隔热",
                    "热工",
                    "太阳辐射",
                    "风压",
                    "风帽",
                    "百叶",
                ),
            ),
            SourceRule("public-building-principle", "all"),
        ),
    ),
    ThemeRule(
        filename="03-校园与教育建筑运维场景.md",
        title="校园与教育建筑运维场景",
        description="适合校园、教学楼、图书馆等场景的环境控制、照明、通风与设备问答。",
        upload_priority="高",
        sources=(
            SourceRule(
                "school-specification",
                "filter",
                (
                    "照明",
                    "采光",
                    "通风",
                    "换气",
                    "噪声",
                    "温度",
                    "空气",
                    "环境",
                    "给水",
                    "排水",
                    "热水",
                    "配电",
                    "电气",
                    "设备",
                    "消防",
                    "安防",
                    "节能",
                ),
            ),
            SourceRule("library-principle", "all"),
        ),
    ),
    ThemeRule(
        filename="04-医疗建筑与洁净环境.md",
        title="医疗建筑与洁净环境",
        description="适合医院、病房、洁净环境、医疗设备配套和运维流程问答。",
        upload_priority="高",
        sources=(
            SourceRule(
                "hospital-building-specification",
                "filter",
                (
                    "病房",
                    "监护",
                    "手术",
                    "洁净",
                    "空调",
                    "通风",
                    "换气",
                    "温度",
                    "湿度",
                    "设备",
                    "医疗气体",
                    "感染",
                    "废弃物",
                    "噪声",
                    "给水",
                    "排水",
                    "能耗",
                ),
            ),
        ),
    ),
    ThemeRule(
        filename="05-居住与住宿建筑运行场景.md",
        title="居住与住宿建筑运行场景",
        description="适合宿舍、住宅、酒店、养老与幼儿建筑的舒适性、通风采光与设备运维问答。",
        upload_priority="中",
        sources=(
            SourceRule(
                "dormitory-building-specification",
                "filter",
                (
                    "照明",
                    "采光",
                    "通风",
                    "换气",
                    "空调",
                    "热水",
                    "节能",
                    "温度",
                    "湿度",
                    "设备",
                    "电气",
                    "配电",
                    "给水",
                    "排水",
                    "噪声",
                    "保温",
                    "隔热",
                ),
            ),
            SourceRule(
                "residential-building-specification",
                "filter",
                (
                    "照明",
                    "采光",
                    "通风",
                    "换气",
                    "空调",
                    "热水",
                    "给水",
                    "排水",
                    "燃气",
                    "电气",
                    "设备",
                    "保温",
                    "隔热",
                    "节能",
                    "噪声",
                    "温度",
                ),
            ),
            SourceRule(
                "hotel-building-specification",
                "filter",
                (
                    "照明",
                    "采光",
                    "通风",
                    "换气",
                    "空调",
                    "热水",
                    "设备",
                    "电气",
                    "给水",
                    "排水",
                    "节能",
                    "噪声",
                    "温度",
                    "消防",
                ),
            ),
            SourceRule(
                "old-people-specification",
                "filter",
                (
                    "照明",
                    "采光",
                    "通风",
                    "换气",
                    "空调",
                    "热水",
                    "设备",
                    "温度",
                    "湿度",
                    "噪声",
                    "给水",
                    "排水",
                    "电气",
                ),
            ),
            SourceRule(
                "nursery-building-specification",
                "filter",
                (
                    "照明",
                    "采光",
                    "通风",
                    "换气",
                    "空调",
                    "热水",
                    "设备",
                    "温度",
                    "湿度",
                    "噪声",
                    "给水",
                    "排水",
                    "电气",
                ),
            ),
        ),
    ),
    ThemeRule(
        filename="06-文化场馆与文保环境控制.md",
        title="文化场馆与文保环境控制",
        description="适合博物馆、剧院、文化中心等专项场景的温湿度、照明、库房与环境控制问答。",
        upload_priority="中",
        sources=(
            SourceRule(
                "museum-design-specification",
                "filter",
                (
                    "温度",
                    "湿度",
                    "通风",
                    "换气",
                    "空调",
                    "照明",
                    "灯光",
                    "文物",
                    "库房",
                    "展厅",
                    "藏品",
                    "空气",
                    "消防",
                    "报警",
                    "设备",
                ),
            ),
            SourceRule(
                "theater-building-specification",
                "filter",
                (
                    "通风",
                    "空调",
                    "照明",
                    "灯光",
                    "温度",
                    "湿度",
                    "设备",
                    "噪声",
                    "消防",
                    "排烟",
                    "机房",
                    "舞台",
                ),
            ),
            SourceRule(
                "cultrual-center-specification",
                "filter",
                (
                    "通风",
                    "空调",
                    "照明",
                    "灯光",
                    "温度",
                    "湿度",
                    "设备",
                    "噪声",
                    "消防",
                    "排烟",
                    "机房",
                ),
            ),
        ),
    ),
    ThemeRule(
        filename="07-车库与消防安全运行.md",
        title="车库与消防安全运行",
        description="适合车库通风排烟、照明配电和消防底线问答。",
        upload_priority="中",
        sources=(
            SourceRule(
                "garage-specification",
                "filter",
                (
                    "通风",
                    "换气",
                    "排烟",
                    "消防",
                    "照明",
                    "设备",
                    "电气",
                    "配电",
                    "噪声",
                    "温度",
                    "报警",
                ),
            ),
            SourceRule("fire-prevention-principle", "all"),
        ),
    ),
)


def clean_text(value: str) -> str:
    value = value.replace("\ufeff", "")
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    value = value.replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "")
    value = value.replace('\\"', '"').replace("\\/", "/")
    value = re.sub(r"\\(?=\d)", "", value)
    value = HTML_BREAK_RE.sub("\n", value)
    value = HTML_TAG_RE.sub("", value)
    value = value.replace("&nbsp;", " ").replace("&#160;", " ")
    value = re.sub(r"[ \t]+\n", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    value = re.sub(r"[ \t]{2,}", " ", value)
    return value.strip()


def parse_sikong_file(path: Path) -> list[dict[str, str]]:
    text = path.read_text(encoding="utf-8")
    entries: list[dict[str, str]] = []
    for index, (question, answer) in enumerate(QA_PATTERN.findall(text), start=1):
        question = clean_text(question)
        answer = clean_text(answer)
        if not question or not answer:
            continue
        entries.append(
            {
                "id": f"{path.stem}-{index}",
                "source_title": path.stem,
                "question": question,
                "answer": answer,
                "text": f"Q: {question}\nA: {answer}",
            }
        )
    return entries


def source_display_name(stem: str) -> str:
    return stem.replace("-", " ").strip()


def select_entries(entries: Iterable[dict[str, str]], rule: SourceRule) -> list[dict[str, str]]:
    entries = list(entries)
    if rule.mode == "all":
        return entries
    selected: list[dict[str, str]] = []
    seen_questions: set[str] = set()
    for item in entries:
        text = f"{item['question']}\n{item['answer']}"
        if any(keyword in text for keyword in rule.keywords):
            if item["question"] in seen_questions:
                continue
            seen_questions.add(item["question"])
            selected.append(item)
    return selected


def ensure_clean_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for child in path.iterdir():
        if child.is_file():
            child.unlink()


def write_markdown_doc(theme: ThemeRule, grouped_entries: dict[str, list[dict[str, str]]]) -> tuple[int, int]:
    total = sum(len(items) for items in grouped_entries.values())
    source_count = len([items for items in grouped_entries.values() if items])
    lines = [
        f"# {theme.title}",
        "",
        f"- 上传优先级：{theme.upload_priority}",
        f"- 适用场景：{theme.description}",
        f"- 收录问答：{total} 条",
        f"- 来源文件：{source_count} 个",
        "",
        "> 建议直接把本文件上传到 RAGFlow 的主知识库，用于建筑能耗解释、异常诊断和运维问答。",
        "",
    ]
    for stem, items in grouped_entries.items():
        if not items:
            continue
        lines.extend(
            [
                f"## 来源：{stem}",
                "",
                f"共 {len(items)} 条问答。",
                "",
            ]
        )
        for index, item in enumerate(items, start=1):
            lines.extend(
                [
                    f"### Q{index}. {item['question']}",
                    "",
                    item["answer"],
                    "",
                ]
            )
    (MAIN_KB_DIR / theme.filename).write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return total, source_count


def write_readme(theme_stats: list[tuple[ThemeRule, int, int]], all_entries: list[dict[str, str]], curated_entries: list[dict[str, str]]) -> None:
    lines = [
        "# 司空知识库上传包",
        "",
        "这套上传包已经按 `建筑能耗 + 智慧运维 + 场景化建筑知识` 做过一次筛选，目标是让你可以直接把文件丢进 RAGFlow，而不是先手工拆 JSON。",
        "",
        "## 推荐上传方式",
        "",
        "主知识库建议名称：`A8-运维与节能知识`",
        "",
        "把 `main-kb/` 目录下的 Markdown 文件全部上传进去即可。建议先上传前 4 份，再补充后面的专项场景文件。",
        "",
        "## 本次整理范围",
        "",
        f"- 司空原始文件：{len(list(RAW_DIR.glob('*.json')))} 个",
        f"- 原始问答总量：{len(all_entries)} 条",
        f"- 精选问答总量：{len(curated_entries)} 条",
        "",
        "## 主知识库文件清单",
        "",
    ]
    for theme, total, source_count in theme_stats:
        lines.extend(
            [
                f"### {theme.filename}",
                "",
                f"- 标题：{theme.title}",
                f"- 优先级：{theme.upload_priority}",
                f"- 来源文件数：{source_count}",
                f"- 问答条数：{total}",
                f"- 说明：{theme.description}",
                "",
            ]
        )
    lines.extend(
        [
            "## 附带结构化文件",
            "",
            f"- `{CURATED_JSONL.relative_to(REPO_ROOT).as_posix()}`：精选问答 JSONL，后续接系统检索时可直接使用",
            f"- `{FULL_JSONL.relative_to(REPO_ROOT).as_posix()}`：全量清洗后的司空问答 JSONL，作为补充参考",
            "",
            "## 暂不建议直接放进主知识库的内容",
            "",
            "- 赛题 PDF、指导手册、直播答疑：更适合单独做“答辩与赛题说明”知识库",
            "- `docs/design-reference/images/`：这是视觉参考，不适合知识检索",
            "- BDG2 原始时序 CSV：这些数据应继续留给分析接口和统计逻辑处理，不建议交给 RAGFlow 做主检索",
            "",
        ]
    )
    (OUTPUT_ROOT / "README.md").write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def main() -> None:
    all_entries_by_source: dict[str, list[dict[str, str]]] = {}
    full_entries: list[dict[str, str]] = []
    for path in sorted(RAW_DIR.glob("*.json")):
        entries = parse_sikong_file(path)
        all_entries_by_source[path.stem] = entries
        full_entries.extend(entries)

    ensure_clean_dir(MAIN_KB_DIR)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)

    curated_entries: list[dict[str, str]] = []
    theme_stats: list[tuple[ThemeRule, int, int]] = []
    seen_texts: set[str] = set()

    for theme in THEMES:
        grouped: dict[str, list[dict[str, str]]] = {}
        for source_rule in theme.sources:
            selected = select_entries(all_entries_by_source[source_rule.stem], source_rule)
            grouped[source_rule.stem] = selected
            for item in selected:
                text_key = item["text"]
                if text_key in seen_texts:
                    continue
                seen_texts.add(text_key)
                curated_entries.append(
                    {
                        **item,
                        "theme": theme.title,
                    }
                )
        total, source_count = write_markdown_doc(theme, grouped)
        theme_stats.append((theme, total, source_count))

    with CURATED_JSONL.open("w", encoding="utf-8") as f:
        for item in curated_entries:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    with FULL_JSONL.open("w", encoding="utf-8") as f:
        for item in full_entries:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    write_readme(theme_stats, full_entries, curated_entries)

    print(f"Generated {len(theme_stats)} main KB documents")
    print(f"Curated Q&A count: {len(curated_entries)}")
    print(f"Full cleaned Q&A count: {len(full_entries)}")
    print(f"Output root: {OUTPUT_ROOT}")


if __name__ == "__main__":
    main()
