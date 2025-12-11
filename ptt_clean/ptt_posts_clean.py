#!/usr/bin/env python3
"""课程版（v2）：清洗 PTT 帖子正文 -> 精简结构化数据。"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:  # Python 3.9+ 原生时区
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - 旧 Python 没有 zoneinfo
    ZoneInfo = None  # type: ignore

import pandas as pd
from bs4 import BeautifulSoup

# -------------------------- 基本配置 -------------------------- #

# 原始 PTT HTML JSONL 输入
INPUT_PATH = Path("PTT爬的帖子数据/合并.jsonl")

# 输出目录与文件名（课程专用）
OUTPUT_DIR = Path("data/clean")
OUTPUT_CSV = OUTPUT_DIR / "ptt_posts_clean_课程_v2.csv"
OUTPUT_JSON = OUTPUT_DIR / "ptt_posts_clean_课程_v2.json"
MISSING_REPORT_CSV = OUTPUT_DIR / "ptt_posts_missing_report_课程_v2.csv"

DEFAULT_PLATFORM = "PTT"
DEFAULT_TIMEZONE = "Asia/Taipei"

# 图片 URL 后缀匹配
IMAGE_SUFFIX_PATTERN = re.compile(
    r"\.(?:jpg|jpeg|png|gif|bmp|webp)(?:\?.*)?$", re.IGNORECASE
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# -------------------------- 工具函数 -------------------------- #


def iter_jsonl(path: Path) -> Iterable[Dict[str, object]]:
    """逐行读取 JSONL，避免一次性加载全部数据。"""
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:  # pragma: no cover - 输入有误时才会发生
                logging.warning("第 %s 行 JSON 解析失败: %s", line_no, exc)


def extract_meta(soup: BeautifulSoup) -> Dict[str, str]:
    """从 PTT HTML 中提取 meta 区块（作者、标题、时间等）。"""
    tags = soup.select("span.article-meta-tag")
    values = soup.select("span.article-meta-value")
    result: Dict[str, str] = {}
    for tag, value in zip(tags, values):
        tag_name = tag.get_text(strip=True)
        result[tag_name] = value.get_text(strip=True)
    return result


def clean_main_content(main_content) -> str:
    """从 #main-content 中去掉 meta 和推文，只保留正文文本。"""
    if main_content is None:
        return ""
    # 移除 meta 标签与推文区块
    for selector in ["span.article-meta-tag", "span.article-meta-value", "div.push"]:
        for node in main_content.select(selector):
            node.decompose()
    # 提取纯文本
    text = main_content.get_text("\n", strip=True)
    # 移除签名档（PTT 以 "--" 作为分隔）
    cut = text.find("--")
    if cut != -1:
        text = text[:cut]
    # 合并多余空行
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_push_stats(soup: BeautifulSoup) -> Dict[str, int]:
    """统计推文区块中的推 / 噓 / 总回复数."""
    like = 0
    boo = 0
    total = 0
    for push in soup.select("div.push"):
        tag = push.select_one("span.push-tag")
        if not tag:
            continue
        tag_text = tag.get_text(strip=True)
        total += 1
        if tag_text == "推":
            like += 1
        elif tag_text == "噓":
            boo += 1
    return {"like": like, "boo": boo, "total": total}


def extract_images(main_content) -> List[str]:
    """从正文中抽取图片 URL 列表（img 标签与超链接）。"""
    urls = set()
    if main_content is None:
        return []
    # <img src="...">
    for img in main_content.select("img"):
        src = img.get("src")
        if src and IMAGE_SUFFIX_PATTERN.search(src):
            urls.add(src.strip())
    # <a href="...">，只保留图片链接
    for anchor in main_content.select("a[href]"):
        href = anchor.get("href")
        if not href:
            continue
        if IMAGE_SUFFIX_PATTERN.search(href.strip()):
            urls.add(href.strip())
    return sorted(urls)


def parse_time(raw: Optional[str]) -> Optional[datetime]:
    """解析 PTT meta 中的时间字段（發文時間）。"""
    if not raw:
        return None
    try:
        ts = datetime.strptime(raw.strip(), "%a %b %d %H:%M:%S %Y")
    except ValueError:
        return None
    if ZoneInfo:
        ts = ts.replace(tzinfo=ZoneInfo(DEFAULT_TIMEZONE))
    return ts


def parse_fetched_time(raw: Optional[str]) -> Optional[datetime]:
    """解析爬虫记录中的抓取时间（created_at）。"""
    if not raw:
        return None
    ts = pd.to_datetime(raw, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def ensure_timezone(dt: datetime) -> datetime:
    """确保 datetime 具备时区信息。"""
    if dt.tzinfo is not None:
        return dt
    if ZoneInfo:
        return dt.replace(tzinfo=ZoneInfo(DEFAULT_TIMEZONE))
    return dt.replace(tzinfo=timezone.utc)


def format_timestamp(dt: Optional[datetime]) -> Optional[str]:
    """将 datetime 转为 'YYYY-MM-DD HH:MM:SS' 字符串。"""
    if not dt:
        return None
    dt = ensure_timezone(dt)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def build_post_id(
    *,
    platform: str,
    post_time: Optional[datetime],
    url: Optional[str],
    fallback_index: int,
) -> str:
    """根据平台、时间与 URL 构造帖子唯一 ID：平台_时间戳_哈希。"""
    timestamp_source = post_time
    if not timestamp_source:
        # 若没有发文/抓取时间，用索引构造一个稳定时间戳
        timestamp_source = datetime.fromtimestamp(fallback_index, tz=timezone.utc)
    timestamp_part = int(ensure_timezone(timestamp_source).timestamp())

    slug_source = url or f"{platform}-{fallback_index}"
    digest = hashlib.sha1(slug_source.encode("utf-8")).hexdigest()[:10]
    return f"{platform}_{timestamp_part}_{digest}"


def _normalize_text_value(value: Optional[object]) -> Optional[object]:
    """将空字符串或只含空白的字符串统一视为缺失值 None，并 strip 文本。"""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return None if not stripped else stripped
    return value


def normalize_text_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    """对指定字段进行字符串标准化处理（去除空字符串 -> None，并去掉首尾空格）。"""
    for column in columns:
        if column in df.columns:
            df[column] = df[column].map(_normalize_text_value)


def build_missing_report(
    columns: Iterable[str],
    *,
    total_raw: int,
    missing_raw: pd.Series,
    total_cleaned: int,
    missing_cleaned: pd.Series,
    total_final: int,
    missing_final: pd.Series,
) -> pd.DataFrame:
    """生成各字段缺失值统计报表（原始 / 清洗后 / 最终数据三阶段对比）。

    - missing_raw：    原始爬取数据中“显性缺失”（NaN / None）的计数。
    - missing_cleaned：完成标准化等清洗后（空字符串已统一为缺失），
                       但尚未填补占位符 / 默认值前的缺失计数。
    - missing_final：  在删除无效记录并完成填补之后，最终可分析数据中的缺失计数。
    """
    total_raw = total_raw or 1
    total_cleaned = total_cleaned or 1
    total_final = total_final or 1

    columns = list(columns)
    report = pd.DataFrame({"column": columns})

    # 原始数据：只统计显性缺失
    report["missing_raw"] = report["column"].map(missing_raw).fillna(0).astype(int)
    report["missing_raw_pct"] = report["missing_raw"] / total_raw * 100

    # 清洗后（标准化、识别隐性缺失之后，但尚未填补默认值）
    report["missing_cleaned"] = (
        report["column"].map(missing_cleaned).fillna(0).astype(int)
    )
    report["missing_cleaned_pct"] = (
        report["missing_cleaned"] / total_cleaned * 100
    )

    # 最终可用于分析的数据（删除无效记录，完成必要填补后）
    report["missing_final"] = report["column"].map(missing_final).fillna(0).astype(int)
    report["missing_final_pct"] = report["missing_final"] / total_final * 100

    return report


# -------------------------- 核心解析 -------------------------- #


def parse_ptt_post(
    html: str, *, url: Optional[str], fetched_at: Optional[str], fallback_index: int
) -> Dict[str, object]:
    """解析单篇 PTT 帖子的 HTML，生成一条结构化记录。"""
    soup = BeautifulSoup(html, "lxml")
    meta = extract_meta(soup)
    main_content = soup.select_one("#main-content")
    push_stats = parse_push_stats(soup)
    content = clean_main_content(main_content)

    # 标题
    title = meta.get("標題") or (soup.title.get_text(strip=True) if soup.title else None)

    # 作者字段形如： "id (昵称)"，只保留 id 和昵称
    author_field = meta.get("作者") or ""
    author_id = author_field.split(" ")[0] if author_field else None
    author_name = None
    if "(" in author_field and ")" in author_field:
        author_name = author_field.split("(", 1)[1].split(")", 1)[0].strip()
    elif author_id:
        author_name = author_id

    # 点赞 / 回复统计
    like_count = push_stats["like"]
    reply_count = push_stats["total"]

    # 图片列表
    images = extract_images(main_content)

    # 时间信息：优先用网页上的發文時間，其次用抓取时间
    post_time_dt = parse_time(meta.get("時間"))
    fetched_time_dt = parse_fetched_time(fetched_at)
    time_for_id = post_time_dt or fetched_time_dt
    if not post_time_dt:
        post_time_dt = fetched_time_dt
    post_time_str = format_timestamp(post_time_dt)

    # 帖子 ID：平台_时间戳_哈希
    post_id = build_post_id(
        platform=DEFAULT_PLATFORM,
        post_time=time_for_id,
        url=url,
        fallback_index=fallback_index,
    )

    return {
        "post_id": post_id,
        "title": title,
        "content": content,
        "post_time": post_time_str,
        "author_id": author_id.lower() if author_id else None,
        "author_name": author_name,
        "post_url": url,
        "images": images,
        "like_count": like_count,
        "reply_count": reply_count,
    }


# -------------------------- 主流程 -------------------------- #


def ensure_output_dir() -> None:
    """确保输出目录存在。"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """清洗帖子正文数据并输出为 CSV / JSON 及缺失值报表。"""
    ensure_output_dir()

    records: List[Dict[str, object]] = []

    # 逐行读取爬虫 JSONL，解析每篇帖子
    for idx, payload in enumerate(iter_jsonl(INPUT_PATH), 1):
        html = payload.get("html")
        if not html:
            continue
        url = payload.get("url")
        created_at = payload.get("created_at")

        record = parse_ptt_post(html, url=url, fetched_at=created_at, fallback_index=idx)
        records.append(record)

        if idx % 1000 == 0:
            logging.info("已处理 %s 篇帖子", idx)

    df = pd.DataFrame(records)

    if df.empty:
        logging.warning("没有可输出的帖子数据，仅写出空文件。")
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        df.to_json(OUTPUT_JSON, orient="records", force_ascii=False, indent=2)
        pd.DataFrame().to_csv(MISSING_REPORT_CSV, index=False, encoding="utf-8-sig")
        return

    # 课程需求字段顺序
    expected_columns = [
        "post_id",
        "title",
        "content",
        "post_time",
        "author_id",
        "author_name",
        "post_url",
        "images",
        "like_count",
        "reply_count",
    ]

    # ---------- 阶段 1：原始数据缺失（显性缺失） ---------- #
    total_raw = len(df)
    # 只对关注的列统计缺失情况；此时仅统计显性 NaN/None
    missing_raw = df[expected_columns].isna().sum()

    # 1) 去重：以 post_id 为唯一键
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["post_id"])
    dedup_removed = before_dedup - len(df)
    if dedup_removed:
        logging.info("去重 post_id 数量：%s", dedup_removed)

    # 2) 文本字段标准化：空字符串 -> None，并去掉首尾空格
    normalize_text_columns(
        df,
        ["title", "content", "author_id", "author_name", "post_url"],
    )

    # ---------- 阶段 2：清洗后缺失（识别隐性缺失，但尚未填补默认值） ---------- #
    total_cleaned = len(df)
    missing_cleaned = df[expected_columns].isna().sum()

    # 3) 基本有效性检查：标题 / 正文 / 链接 三者全部缺失时，视为无效帖子
    invalid_mask = df["title"].isna() & df["content"].isna() & df["post_url"].isna()
    if invalid_mask.any():
        removed = int(invalid_mask.sum())
        df = df.loc[~invalid_mask].copy()
        logging.info("删除无效帖子 %s 条", removed)

    if df.empty:
        logging.warning("清理后没有剩余帖子。输出空结果与缺失报表。")
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        df.to_json(OUTPUT_JSON, orient="records", force_ascii=False, indent=2)
        pd.DataFrame().to_csv(MISSING_REPORT_CSV, index=False, encoding="utf-8-sig")
        return

    # 4) 填补缺失标题
    title_fill_mask = df["title"].isna()
    title_fill_count = int(title_fill_mask.sum())
    if title_fill_count:
        df.loc[title_fill_mask, "title"] = "(no title)"
        logging.info("title 缺失以占位符填补 %s 条", title_fill_count)

    # 5) 点赞数 / 回复数 缺失统一填 0
    for field in ["like_count", "reply_count"]:
        if field in df.columns:
            missing_count = int(df[field].isna().sum())
            df[field] = df[field].fillna(0).astype(int)
            if missing_count:
                logging.info("%s 缺失填补 0 -> %s 条", field, missing_count)

    # 保证字段顺序与课程需求一致
    df = df[expected_columns]

    # ---------- 阶段 3：最终数据缺失（删除无效记录并填补后的统计） ---------- #
    total_final = len(df)
    missing_final = df.isna().sum()
    report = build_missing_report(
        expected_columns,
        total_raw=total_raw,
        missing_raw=missing_raw,
        total_cleaned=total_cleaned,
        missing_cleaned=missing_cleaned,
        total_final=total_final,
        missing_final=missing_final,
    )
    report.to_csv(MISSING_REPORT_CSV, index=False, encoding="utf-8-sig")
    logging.info("缺失值报表输出 -> %s", MISSING_REPORT_CSV)

    # 写出 CSV / JSON
    df_csv = df.copy()
    if "images" in df_csv.columns:
        # CSV 中无法直接存数组，转为 JSON 字符串以便后续解析
        df_csv["images"] = df_csv["images"].apply(json.dumps)

    df_csv.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_json(OUTPUT_JSON, orient="records", force_ascii=False, indent=2)

    logging.info(
        "完成！共输出 %s 条帖子 -> %s / %s",
        len(df),
        OUTPUT_CSV,
        OUTPUT_JSON,
    )


if __name__ == "__main__":
    main()
