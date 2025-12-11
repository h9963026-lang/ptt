#!/usr/bin/env python3
"""Traverse all PTT board classification pages and export board info to Excel."""

from __future__ import annotations

import argparse
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse
import threading

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook


BASE_URL = "https://www.ptt.cc"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}


_thread_local = threading.local()


@dataclass(frozen=True)
class BoardEntry:
    board: str
    title: str
    url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl every PTT board classification (/cls/) page and export board list to Excel."
    )
    parser.add_argument(
        "--start",
        nargs="+",
        help="Optional starting /cls/ paths or URLs. Defaults to auto-discover from the PTT index page.",
    )
    parser.add_argument(
        "--out",
        default="ptt_all_boards.xlsx",
        help="Output Excel path (default: %(default)s).",
    )
    parser.add_argument(
        "--sheet-name",
        default="ptt_boards",
        help="Worksheet name (default: %(default)s).",
    )
    parser.add_argument(
        "--delay-min",
        type=float,
        default=0.3,
        help="Minimum delay between HTTP requests in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--delay-max",
        type=float,
        default=0.8,
        help="Maximum delay between HTTP requests in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="HTTP timeout in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--max-cls-pages",
        type=int,
        default=0,
        help="Optional limit on number of /cls/ pages to visit (0 = no limit).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=6,
        help="Number of concurrent classification requests (default: %(default)s).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress for every classification page.",
    )
    return parser.parse_args()


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    session.cookies.set("over18", "1", domain="ptt.cc")
    session.cookies.set("over18", "1", domain=".ptt.cc")
    return session


def get_thread_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = build_session()
        _thread_local.session = session
    return session


def normalize_cls_url(value: str) -> str:
    if not value:
        return ""
    absolute = urljoin(BASE_URL, value)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""
    if parsed.netloc not in {"ptt.cc", "www.ptt.cc"}:
        return ""
    if not parsed.path.startswith("/cls/"):
        return ""
    normalized = parsed._replace(fragment="", params="", query=parsed.query or "")
    return normalized.geturl()


def normalize_board_url(value: str) -> Optional[str]:
    if not value:
        return None
    absolute = urljoin(BASE_URL, value)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.netloc not in {"ptt.cc", "www.ptt.cc"}:
        return None
    if not parsed.path.startswith("/bbs/") or not parsed.path.endswith("/index.html"):
        return None
    return parsed._replace(fragment="", params="").geturl()


def get_soup(session: requests.Session, url: str, timeout: float) -> Optional[BeautifulSoup]:
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True)
        except requests.RequestException:
            time.sleep(0.6 * (attempt + 1))
            continue
        if resp.status_code == 200:
            resp.encoding = resp.apparent_encoding or "utf-8"
            return BeautifulSoup(resp.text, "html.parser")
        if resp.status_code == 404:
            return None
        time.sleep(0.6 * (attempt + 1))
    return None


def discover_cls_roots(session: requests.Session, timeout: float) -> List[str]:
    soup = get_soup(session, f"{BASE_URL}/bbs/index.html", timeout=timeout)
    roots: Set[str] = set()
    if soup:
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            text = (a.get_text() or "").strip()
            norm = normalize_cls_url(href)
            if norm:
                roots.add(norm)
            if text and "看板列表" in text and href:
                norm = normalize_cls_url(href)
                if norm:
                    roots.add(norm)
    if not roots:
        roots.add(f"{BASE_URL}/cls/1")
    return sorted(roots)


def ensure_start_urls(
    session: requests.Session, provided: Optional[Sequence[str]], timeout: float
) -> List[str]:
    if provided:
        normalized = []
        for value in provided:
            norm = normalize_cls_url(value)
            if norm:
                normalized.append(norm)
        return sorted(set(normalized))
    return discover_cls_roots(session, timeout=timeout)


def parse_boards_from_cls(soup: BeautifulSoup) -> List[BoardEntry]:
    entries: List[BoardEntry] = []
    seen: Set[str] = set()

    for block in soup.select("div.b-ent"):
        board_el = block.select_one(".board-name")
        link_el = block.select_one("a.board[href]")
        if not board_el or not link_el:
            continue
        board_id = board_el.get_text(strip=True)
        board_url = normalize_board_url(link_el.get("href"))
        if not board_id or not board_url or board_id in seen:
            continue
        title_el = block.select_one(".board-title")
        title = title_el.get_text(strip=True) if title_el else ""
        entries.append(BoardEntry(board=board_id, title=title, url=board_url))
        seen.add(board_id)

    for anchor in soup.select("a[href]"):
        href = anchor.get("href") or ""
        board_url = normalize_board_url(href)
        if not board_url:
            continue
        path_parts = urlparse(board_url).path.strip("/").split("/")
        board_id = path_parts[1] if len(path_parts) >= 3 else ""
        if not board_id or board_id in seen:
            continue
        title = anchor.get_text(strip=True)
        entries.append(BoardEntry(board=board_id, title=title, url=board_url))
        seen.add(board_id)

    return entries


def extract_cls_links(soup: BeautifulSoup) -> Set[str]:
    links: Set[str] = set()
    for anchor in soup.select("a[href]"):
        href = anchor.get("href") or ""
        norm = normalize_cls_url(href)
        if norm:
            links.add(norm)
    return links


def crawl_boards(
    start_urls: Iterable[str],
    delay_min: float,
    delay_max: float,
    timeout: float,
    max_cls_pages: int,
    max_workers: int,
    verbose: bool,
) -> Dict[str, BoardEntry]:
    pending: Set[str] = {url for url in start_urls if url}
    visited_cls: Set[str] = set()
    boards: Dict[str, BoardEntry] = {}
    cls_count = 0
    max_workers = max(1, int(max_workers))
    max_cls_pages = max(0, int(max_cls_pages))
    print_lock = threading.Lock()

    def fetch_classification(url: str) -> Tuple[List[BoardEntry], Set[str]]:
        session = get_thread_session()
        delay = random.uniform(delay_min, delay_max)
        if delay > 0:
            time.sleep(delay)
        soup = get_soup(session, url, timeout=timeout)
        if soup is None:
            return [], set()
        return parse_boards_from_cls(soup), extract_cls_links(soup)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        in_flight: Dict[object, str] = {}

        def schedule(url: str) -> None:
            if url in visited_cls:
                return
            visited_cls.add(url)
            future = executor.submit(fetch_classification, url)
            in_flight[future] = url

        stop = False
        try:
            while in_flight or pending:
                if stop:
                    break
                for url in list(pending):
                    pending.discard(url)
                    schedule(url)
                if not in_flight:
                    break

                for future in as_completed(list(in_flight.keys())):
                    url = in_flight.pop(future)
                    try:
                        board_entries, next_cls = future.result()
                    except Exception as exc:  # noqa: BLE001
                        if verbose:
                            with print_lock:
                                print(f"[!] Failed {url}: {exc}")
                        continue

                    cls_count += 1
                    added = 0
                    for entry in board_entries:
                        if entry.board not in boards:
                            boards[entry.board] = entry
                            added += 1

                    for nxt in next_cls:
                        if nxt not in visited_cls and nxt not in pending:
                            pending.add(nxt)

                    if verbose:
                        with print_lock:
                            print(
                                f"[# {cls_count}] {url} -> +{added} "
                                f"(total {len(boards)}) queue={len(pending)}"
                            )

                    if max_cls_pages and cls_count >= max_cls_pages:
                        pending.clear()
                        stop = True
                        break
                    break
        finally:
            for future in in_flight:
                future.cancel()

    return boards


def write_excel(path: str, sheet_name: str, boards: Dict[str, BoardEntry]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31] or "Sheet1"
    ws.append(["board", "title", "url"])
    for entry in sorted(boards.values(), key=lambda b: b.board.lower()):
        ws.append([entry.board, entry.title, entry.url])
    wb.save(path)


def main() -> None:
    args = parse_args()
    delay_min = max(0.0, args.delay_min)
    delay_max = max(delay_min, args.delay_max)
    session = build_session()
    start_urls = ensure_start_urls(session, args.start, timeout=args.timeout)
    if not start_urls:
        print("No valid /cls/ start URLs found.", file=sys.stderr)
        sys.exit(1)

    boards = crawl_boards(
        start_urls=start_urls,
        delay_min=delay_min,
        delay_max=delay_max,
        timeout=args.timeout,
        max_cls_pages=max(0, args.max_cls_pages),
        max_workers=args.max_workers,
        verbose=args.verbose,
    )
    if not boards:
        print("Failed to collect any boards.", file=sys.stderr)
        sys.exit(1)

    write_excel(args.out, args.sheet_name, boards)
    print(f"[+] Collected {len(boards)} boards")
    print(f"[+] Saved Excel to {args.out}")


if __name__ == "__main__":
    main()
