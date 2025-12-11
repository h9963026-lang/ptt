#!/usr/bin/env python3
"""课程版（v2）：清洗 PTT 推文 -> 评论数据集。"""

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

# 输出目录与文件名
OUTPUT_DIR = Path("data/clean")
OUTPUT_CSV = OUTPUT_DIR / "ptt_comments_clean_课程_v2.csv"
OUTPUT_JSON = OUTPUT_DIR / "ptt_comments_clean_课程_v2.json"
MISSING_REPORT_CSV = OUTPUT_DIR / "ptt_comments_missing_report_课程_v2.csv"

# 平台与时间相关设置
DEFAULT_PLATFORM = "PTT"
DEFAULT_TIMEZONE = "Asia/Taipei"

# PTT 推文时间样式，例如：" 01/02 15:04"
PUSH_TIME_PATTERN = re.compile(
    r"(?:(?P<ip>\d{1,3}(?:\.\d{1,3}){3})\s+)?"
    r"(?P<month>\d{1,2})/(?P<day>\d{1,2})\s+"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{1,2})"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# -------------------------- 通用工具函数 -------------------------- #


def iter_jsonl(path: Path) -> Iterable[Dict[str, object]]:
    """逐行读取 JSONL 文件，避免一次读入全部造成内存压力。"""
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:  # pragma: no cover - 仅在输入有误时触发
                logging.warning("第 %s 行 JSON 解析失败: %s", line_no, exc)


def parse_post_time(raw: Optional[str]) -> Optional[datetime]:
    """解析 PTT 网页 meta 中的「时间」字段。

    PTT 标准格式：'Mon Jan  2 15:04:05 2006'
    """
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
    """解析爬虫记录中的抓取时间（如 created_at），并转为 datetime。"""
    if not raw:
        return None
    ts = pd.to_datetime(raw, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def ensure_timezone(dt: datetime) -> datetime:
    """确保 datetime 具备时区信息，默认使用台北时区。"""
    if dt.tzinfo is not None:
        return dt
    if ZoneInfo:
        return dt.replace(tzinfo=ZoneInfo(DEFAULT_TIMEZONE))
    return dt.replace(tzinfo=timezone.utc)


def format_timestamp(dt: Optional[datetime]) -> Optional[str]:
    """将 datetime 转为字符串格式 'YYYY-MM-DD HH:MM:SS'。"""
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
    """根据平台、发文/抓取时间与 URL 构造帖子唯一 ID。"""
    timestamp_source = post_time
    if not timestamp_source:
        # 若无发文时间，使用索引构造一个稳定的时间戳作为替代
        timestamp_source = datetime.fromtimestamp(fallback_index, tz=timezone.utc)
    timestamp_part = int(ensure_timezone(timestamp_source).timestamp())

    slug_source = url or f"{platform}-{fallback_index}"
    digest = hashlib.sha1(slug_source.encode("utf-8")).hexdigest()[:10]
    return f"{platform}_{timestamp_part}_{digest}"


def build_comment_id(post_id: str, seq: int) -> str:
    """组合评论唯一 ID：{post_id}_push_{序号}，序号为四位数。"""
    return f"{post_id}_push_{seq:04d}"


def _normalize_text_value(value: Optional[object]) -> Optional[object]:
    """将空字符串或只含空白的字符串统一视为缺失值 None，并 strip 文本。"""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return None if not stripped else stripped
    return value


def normalize_text_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    """对指定字段进行字符串标准化处理：strip 且空串 -> None。"""
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
                       但尚未删除无效记录前的缺失计数。
    - missing_final：  在删除无效记录并完成必要填补之后，最终可分析数据中的缺失计数。
    """
    total_raw = total_raw or 1
    total_cleaned = total_cleaned or 1
    total_final = total_final or 1

    columns = list(columns)
    report = pd.DataFrame({"column": columns})

    # 原始数据：只统计显性缺失
    report["missing_raw"] = report["column"].map(missing_raw).fillna(0).astype(int)
    report["missing_raw_pct"] = report["missing_raw"] / total_raw * 100

    # 清洗后（标准化、识别隐性缺失之后，但尚未删除无效记录）
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


# -------------------------- 推文解析（PTT 专用） -------------------------- #


def parse_push_time(raw: Optional[str], base_dt: Optional[datetime], fallback_index: int) -> datetime:
    """解析 PTT 推文的时间字段，补足年份并加上时区信息。

    PTT 推文时间通常只有「MM/DD HH:MM」，需要结合贴文年份估计完整时间。
    - base_dt: 以贴文时间或抓取时间作为同一年的参考
    - 若月份与 base_dt 相差过大，代表跨年，做简单补年份调整
    """
    # 预设回退：以 fallback_index 构造一个稳定时间
    now = datetime.fromtimestamp(fallback_index, tz=timezone.utc)
    base = ensure_timezone(base_dt) if base_dt else now

    if not raw:
        return base
    match = PUSH_TIME_PATTERN.search(raw.strip())
    if not match:
        return base

    month = int(match.group("month"))
    day = int(match.group("day"))
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))

    year = base.year
    # 若 base 月份与推文月份差距过大，视为跨年（例如 12 -> 1）
    if base.month - month > 6:
        year += 1

    try:
        result = datetime(year, month, day, hour, minute)
    except ValueError:
        return base

    if ZoneInfo:
        result = result.replace(tzinfo=ZoneInfo(DEFAULT_TIMEZONE))
    else:
        result = result.replace(tzinfo=timezone.utc)
    return result


def parse_push_comments(
    soup: BeautifulSoup,
    *,
    post_id: str,
    base_dt: Optional[datetime],
    fallback_index: int,
) -> List[Dict[str, object]]:
    """从 PTT 贴文 HTML 的 BeautifulSoup 对象中解析所有推文（评论）数据。

    字段说明：
    - comment_id: {post_id}_push_{四位序号}
    - post_id: 所属帖子 ID
    - post_time: 评论发布时间（YYYY-MM-DD HH:MM:SS）
    - comment_type: 推 / 噓 / →（来自 span.push-tag）
    - author_id: 推文账号 ID
    - content: 评论内容
    - content_length: 评论内容长度（字符数）
    """
    results: List[Dict[str, object]] = []
    pushes = soup.select("div.push")
    if not pushes:
        return results

    if not base_dt:
        base_dt = datetime.fromtimestamp(fallback_index, tz=timezone.utc)

    for seq, push in enumerate(pushes, 1):
        tag_el = push.select_one("span.push-tag")
        user_el = push.select_one("span.push-userid")
        content_el = push.select_one("span.push-content")
        time_el = push.select_one("span.push-ipdatetime")

        # 推文类型标记：推 / 噓 / → 等
        tag_text = tag_el.get_text(strip=True) if tag_el else ""
        comment_type = tag_text or None

        # PTT 推文账号 ID（没有额外的昵称）
        author_id = user_el.get_text(strip=True) if user_el else None

        # 推文内容（纯文本）
        raw_content = content_el.get_text(" ", strip=True) if content_el else ""

        comment_time_dt = parse_push_time(
            time_el.get_text() if time_el else None,
            base_dt,
            fallback_index + seq,
        )

        comment_id = build_comment_id(post_id, seq)
        post_time_str = format_timestamp(comment_time_dt)

        results.append(
            {
                "comment_id": comment_id,
                "post_id": post_id,
                "post_time": post_time_str,
                "comment_type": comment_type,
                "author_id": author_id.lower() if author_id else None,
                "content": raw_content,
                "content_length": len(raw_content),
            }
        )

    return results


# -------------------------- 主流程 -------------------------- #


def ensure_output_dir() -> None:
    """确保输出目录存在。"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """清洗评论数据并输出为 CSV / JSON 及缺失值报表。"""
    ensure_output_dir()

    records: List[Dict[str, object]] = []

    # 逐篇贴文解析，从中抽取所有推文（评论）
    for idx, payload in enumerate(iter_jsonl(INPUT_PATH), 1):
        html = payload.get("html")
        if not html:
            continue

        url = payload.get("url")
        fetched_at = payload.get("created_at")

        soup = BeautifulSoup(html, "lxml")

        # 抽取贴文 meta 信息，主要是发文时间
        meta: Dict[str, str] = {}
        try:
            for tag, value in zip(
                soup.select("span.article-meta-tag"),
                soup.select("span.article-meta-value"),
            ):
                meta[tag.get_text(strip=True)] = value.get_text(strip=True)
        except Exception:
            meta = {}

        post_time_dt = parse_post_time(meta.get("時間"))
        fetched_time_dt = parse_fetched_time(fetched_at)

        # 构造帖子 ID，与原贴文清洗脚本一致，方便后续关联
        post_id = build_post_id(
            platform=DEFAULT_PLATFORM,
            post_time=post_time_dt or fetched_time_dt,
            url=url,
            fallback_index=idx,
        )

        comments = parse_push_comments(
            soup,
            post_id=post_id,
            base_dt=post_time_dt or fetched_time_dt,
            fallback_index=idx * 1000,
        )
        records.extend(comments)

        if idx % 1000 == 0:
            logging.info("已处理 %s 篇贴文 -> 累积 %s 条评论", idx, len(records))

    # 转为 DataFrame 以便后续清洗与输出
    df = pd.DataFrame(records)

    if df.empty:
        logging.warning("没有可输出的评论数据，仅写出空文件。")
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        df.to_json(OUTPUT_JSON, orient="records", force_ascii=False, indent=2)
        pd.DataFrame().to_csv(MISSING_REPORT_CSV, index=False, encoding="utf-8-sig")
        return

    # 课程需求字段顺序
    expected_columns = [
        "comment_id",
        "post_id",
        "post_time",
        "comment_type",
        "author_id",
        "content",
        "content_length",
    ]

    # ---------- 阶段 1：原始数据缺失（显性缺失） ---------- #
    total_raw = len(df)
    missing_raw = df[expected_columns].isna().sum()

    # 1) 去重：以 comment_id 为唯一键
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["comment_id"])
    dedup_removed = before_dedup - len(df)
    if dedup_removed:
        logging.info("去重 comment_id 数量：%s", dedup_removed)

    # 2) 文本字段标准化：strip + 空字符串 -> None
    normalize_text_columns(
        df,
        ["author_id", "content"],
    )

    # ---------- 阶段 2：清洗后缺失（识别隐性缺失，但尚未删除无效记录） ---------- #
    total_cleaned = len(df)
    missing_cleaned = df[expected_columns].isna().sum()

    # 3) 基本有效性检查：必须有内容才保留
    invalid_mask = df["content"].isna()
    if invalid_mask.any():
        removed = int(invalid_mask.sum())
        df = df.loc[~invalid_mask].copy()
        logging.info("删除无内容评论 %s 条", removed)

    if df.empty:
        logging.warning("清理后没有剩余评论。输出空结果与缺失报表。")
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        df.to_json(OUTPUT_JSON, orient="records", force_ascii=False, indent=2)
        pd.DataFrame().to_csv(MISSING_REPORT_CSV, index=False, encoding="utf-8-sig")
        return

    # 4) 重新计算内容长度，确保与 content 一致
    df["content_length"] = df["content"].apply(
        lambda x: len(x) if isinstance(x, str) else 0
    )

    # 保证字段顺序与需求一致
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
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_json(OUTPUT_JSON, orient="records", force_ascii=False, indent=2)

    logging.info(
        "完成！共输出 %s 条评论 -> %s / %s",
        len(df),
        OUTPUT_CSV,
        OUTPUT_JSON,
    )


if __name__ == "__main__":
    main()

