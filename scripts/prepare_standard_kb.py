from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

from pypdf import PdfReader


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT_FALLBACK = Path(r"D:/Project/2026/A8")
RAW_STANDARD_DIR = PROJECT_ROOT_FALLBACK / "docs" / "a08建筑能源智能管理与运营优化关键技术研究" / "建筑运维标准文档" / "规范标准"
OUTPUT_ROOT = REPO_ROOT / "docs" / "ragflow" / "standard-kb-pack"
MAIN_KB_DIR = OUTPUT_ROOT / "main-kb"


@dataclass(frozen=True)
class StandardDoc:
    filename: str
    standard_code: str
    title: str
    domain: str


FIRST_BATCH: tuple[StandardDoc, ...] = (
    StandardDoc("国家标准—《空调通风系统运行管理标准》GB50365-2019.pdf", "GB 50365-2019", "空调通风系统运行管理标准", "hvac"),
    StandardDoc("国家标准—《民用建筑供暖通风与空气调节设计规范》GB 50736-2012.pdf", "GB 50736-2012", "民用建筑供暖通风与空气调节设计规范", "hvac"),
    StandardDoc("国家标准—《民用建筑电气设计标准》GB 51348-2019.pdf", "GB 51348-2019", "民用建筑电气设计标准", "electrical"),
    StandardDoc("国家标准—《民用建筑能耗分类及表示方法》GB T 34913-2017.pdf", "GB/T 34913-2017", "民用建筑能耗分类及表示方法", "energy_quota"),
    StandardDoc("国强规范—《建筑节能与可再生能源利用通用规范》GB 55015-2021.pdf", "GB 55015-2021", "建筑节能与可再生能源利用通用规范", "energy_quota"),
    StandardDoc("国家标准—《建筑节能基本术语标准》GB T 51140-2015.pdf", "GB/T 51140-2015", "建筑节能基本术语标准", "general_terms"),
    StandardDoc("国家标准—《智能服务 预测性维护 通用要求》GB T 40571-2021.pdf", "GB/T 40571-2021", "智能服务 预测性维护 通用要求", "facility_management"),
    StandardDoc("地方标准—【山东省】《教育机构能源消耗定额标准》DB37T 2671-2019.pdf", "DB37T 2671-2019", "教育机构能源消耗定额标准", "energy_quota"),
    StandardDoc("地方标准—【山东省】《公共建筑节能监测系统技术标准》DB37 T 5197-2021.pdf", "DB37 T 5197-2021", "公共建筑节能监测系统技术标准", "monitoring"),
)

EXCLUDED_KEYWORDS = ("彩页", "离心泵", ".png")

SECTION_RE = re.compile(r"^(第[一二三四五六七八九十百]+[章节篇].*|[0-9]+(?:\.[0-9]+){0,2}\s+.*)$")


def slugify(code: str, title: str) -> str:
    raw = f"{code}-{title}"
    cleaned = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", raw)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or "standard-doc"


def extract_pdf_text(path: Path) -> str:
    reader = PdfReader(str(path))
    texts: list[str] = []
    for page in reader.pages:
        text = (page.extract_text() or "").replace("\x00", " ").strip()
        if text:
            texts.append(text)
    text = "\n".join(texts)
    text = re.sub(r"\r", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def split_chunks(text: str, size_limit: int = 1200) -> list[tuple[str, str]]:
    if not text:
        return []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    chunks: list[tuple[str, str]] = []
    current_title = "正文摘录"
    current_lines: list[str] = []
    current_len = 0

    def flush() -> None:
        nonlocal current_lines, current_len
        body = "\n".join(current_lines).strip()
        if body:
            chunks.append((current_title, body))
        current_lines = []
        current_len = 0

    for line in lines:
        if SECTION_RE.match(line) and current_lines:
            flush()
            current_title = line
            continue
        current_lines.append(line)
        current_len += len(line)
        if current_len >= size_limit:
            flush()
    flush()
    return chunks


def render_doc(doc: StandardDoc, pdf_path: Path, text: str) -> str:
    chunks = split_chunks(text)
    lines = [
        f"# {doc.standard_code} {doc.title}",
        "",
        f"- `doc_title`: {doc.title}",
        f"- `standard_code`: {doc.standard_code}",
        "- `source_type`: standard",
        f"- `domain`: {doc.domain}",
        f"- `source_file`: {pdf_path.name}",
        "",
    ]
    if not chunks:
        lines.extend(
            [
                "> 当前 PDF 文本抽取结果较弱，已保留标准元信息。建议后续补 OCR 或人工清洗后再重切。",
                "",
            ]
        )
        return "\n".join(lines) + "\n"

    lines.append("## 标准摘录")
    lines.append("")
    for index, (section, body) in enumerate(chunks, start=1):
        lines.append(f"### 片段 {index}")
        lines.append("")
        lines.append(f"- `chapter_or_section`: {section}")
        lines.append("")
        lines.append(body)
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def render_readme(results: list[dict[str, object]], excluded_files: list[str]) -> str:
    included_lines = []
    for item in results:
        status = "已提取正文" if item["chunk_count"] else "仅保留元信息"
        included_lines.append(f"- `{item['filename']}`：{item['standard_code']}，{item['domain']}，{status}")
    excluded_lines = [f"- `{name}`" for name in excluded_files]
    return "\n".join(
        [
            "# 标准规范库上传包",
            "",
            "本目录用于 A8 第二知识源“标准规范库”上传到 RAGFlow。",
            "",
            "## 首批纳入文件",
            "",
            *included_lines,
            "",
            "## 本轮排除文件",
            "",
            *excluded_lines,
            "",
            "## 上传建议",
            "",
            "- 在 RAGFlow 新建独立数据集，例如 `A8-标准规范库`。",
            "- 将 `main-kb/` 目录下的 Markdown 文件上传到该数据集。",
            "- 上传完成后，把对应 dataset id 写入 `.env` 的 `RAGFLOW_STANDARD_DATASET_IDS`。",
            "- 当前若某些文件只有元信息、没有正文摘录，说明原 PDF 为扫描版或抽取质量较弱，后续建议补 OCR 后替换同名文件。",
            "",
        ]
    ).strip() + "\n"


def main() -> None:
    if not RAW_STANDARD_DIR.exists():
        raise FileNotFoundError(f"Missing standard docs dir: {RAW_STANDARD_DIR}")

    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    MAIN_KB_DIR.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, object]] = []
    for doc in FIRST_BATCH:
        pdf_path = RAW_STANDARD_DIR / doc.filename
        if not pdf_path.exists():
            raise FileNotFoundError(f"Missing standard pdf: {pdf_path}")
        text = extract_pdf_text(pdf_path)
        markdown = render_doc(doc, pdf_path, text)
        out_name = f"{slugify(doc.standard_code, doc.title)}.md"
        (MAIN_KB_DIR / out_name).write_text(markdown, encoding="utf-8")
        results.append(
            {
                "filename": doc.filename,
                "standard_code": doc.standard_code,
                "title": doc.title,
                "domain": doc.domain,
                "chunk_count": len(split_chunks(text)),
                "text_extracted": bool(text),
                "output_file": out_name,
            }
        )

    excluded_files = sorted(
        file.name
        for file in RAW_STANDARD_DIR.iterdir()
        if file.is_file() and file.name not in {doc.filename for doc in FIRST_BATCH}
    )
    (OUTPUT_ROOT / "README.md").write_text(render_readme(results, excluded_files), encoding="utf-8")
    print(json.dumps({"generated_docs": len(results), "output_dir": str(OUTPUT_ROOT), "items": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
