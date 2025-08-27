
"""
scryfall_db.py — Build and query a local MTG card database from Scryfall bulk data.

Features
- Downloads Scryfall bulk "default_cards" (all unique cards) JSON.
- Creates/updates a local SQLite database with useful fields + indexes.
- Stores Scryfall metadata (updated_at, download_uri, etag) to support idempotent updates.
- Provides fast CLI searches (by name, type, CMC range, colors, color identity).
- Pretty-prints results and supports a "card" detail view by exact/partial name.
- Optional: run arbitrary SQL with a safe read-only "query" subcommand.

Usage examples
  # Create or update the database to the latest Scryfall snapshot
  python scryfall_db.py update --db mtg_cards.db

  # Search by (partial) name
  python scryfall_db.py search-name --db mtg_cards.db --name "lightning bolt" --limit 10

  # Search by type line contains
  python scryfall_db.py search-type --db mtg_cards.db --type "Dragon"

  # Search by CMC range
  python scryfall_db.py search-cmc --db mtg_cards.db --min 3 --max 5

  # Search by exact color identity (e.g., WU) or subset (--subset)
  python scryfall_db.py search-colors --db mtg_cards.db --id "WU"         # exactly WU
  python scryfall_db.py search-colors --db mtg_cards.db --id "W" --subset # mono-white or white-inclusive

  # Full card details by name (exact match preferred, falls back to LIKE)
  python scryfall_db.py card --db mtg_cards.db --name "Lightning Bolt"

  # Run a read-only SQL (SELECT only)
  python scryfall_db.py query --db mtg_cards.db --sql "SELECT name, cmc FROM cards WHERE type_line LIKE '%Dragon%' ORDER BY cmc LIMIT 5;"
"""

import argparse
import contextlib
import datetime as dt
import gzip
import io
import json
import os
import re
import sqlite3
import sys
import textwrap
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import requests  # type: ignore
except Exception as e:
    print("This script requires the 'requests' package. Install with: pip install requests", file=sys.stderr)
    raise

SCRYFALL_BULK_INDEX = "https://api.scryfall.com/bulk-data"
TARGET_BULK_TYPE = "default_cards"  # unique cards suitable for most apps
DEFAULT_DB = "mtg_cards.db"
BATCH_SIZE = 1000
TIMEOUT = (10, 300)  # (connect, read)

def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # Cards table (selected useful fields; extend as needed)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cards (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        name_norm TEXT NOT NULL,
        set_code TEXT,
        set_name TEXT,
        collector_number TEXT,
        lang TEXT,
        released_at TEXT,
        layout TEXT,
        mana_cost TEXT,
        cmc REAL,
        type_line TEXT,
        oracle_text TEXT,
        power TEXT,
        toughness TEXT,
        colors TEXT,            -- JSON array of color letters
        color_identity TEXT,    -- JSON array of color letters
        rarity TEXT,
        keywords TEXT,          -- JSON array of strings
        image_uri TEXT,         -- normal image if present
        legalities TEXT,        -- JSON object
        prices TEXT,            -- JSON object
        json_raw TEXT           -- original JSON as string (for completeness)
    );
    """)

    # Metadata table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """)

    # Helpful indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cards_name_norm ON cards(name_norm);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cards_type ON cards(type_line);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cards_cmc ON cards(cmc);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cards_set ON cards(set_code);")

    conn.commit()

def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.lower()).strip()

def get_meta(conn: sqlite3.Connection, key: str) -> Optional[str]:
    cur = conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;", (key, value))
    conn.commit()

def fetch_bulk_index() -> Dict[str, Any]:
    resp = requests.get(SCRYFALL_BULK_INDEX, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def select_default_cards_entry(index_json: Dict[str, Any]) -> Dict[str, Any]:
    for item in index_json.get("data", []):
        if item.get("type") == TARGET_BULK_TYPE:
            return item
    raise RuntimeError(f"Could not find bulk entry with type={TARGET_BULK_TYPE}")

def download_bulk_json(download_uri: str) -> bytes:
    """
    Download the bulk file. Handles gzip if server sends compressed content.
    Returns raw bytes of the JSON (decompressed if needed).
    """
    with requests.get(download_uri, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        content = r.raw.read()
        # Attempt to decompress if it's gzipped (some mirrors may serve .json.gz)
        try:
            return gzip.decompress(content)
        except OSError:
            return content

def parse_cards_bytes(data: bytes) -> List[Dict[str, Any]]:
    """
    Parse JSON array of card objects. Scryfall bulk 'default_cards' is a JSON array.
    This loads into memory once; acceptable for most machines. If you want streaming,
    consider ijson (additional dependency).
    """
    return json.loads(data.decode("utf-8"))

def card_row_from_json(card: Dict[str, Any]) -> Tuple:
    image_uri = None
    if isinstance(card.get("image_uris"), dict):
        image_uri = card["image_uris"].get("normal") or card["image_uris"].get("large") or card["image_uris"].get("small")

    row = (
        card["id"],
        card.get("name"),
        normalize_name(card.get("name", "")),
        card.get("set"),
        card.get("set_name"),
        card.get("collector_number"),
        card.get("lang"),
        card.get("released_at"),
        card.get("layout"),
        card.get("mana_cost"),
        card.get("cmc"),
        card.get("type_line"),
        card.get("oracle_text"),
        card.get("power"),
        card.get("toughness"),
        json.dumps(card.get("colors")) if card.get("colors") is not None else None,
        json.dumps(card.get("color_identity")) if card.get("color_identity") is not None else None,
        card.get("rarity"),
        json.dumps(card.get("keywords")) if card.get("keywords") is not None else None,
        image_uri,
        json.dumps(card.get("legalities")) if card.get("legalities") is not None else None,
        json.dumps(card.get("prices")) if card.get("prices") is not None else None,
        json.dumps(card, ensure_ascii=False),
    )
    return row

def upsert_cards(conn: sqlite3.Connection, cards: Iterable[Dict[str, Any]], batch_size: int = BATCH_SIZE) -> int:
    sql = """
    INSERT INTO cards (
        id, name, name_norm, set_code, set_name, collector_number, lang, released_at, layout, mana_cost, cmc,
        type_line, oracle_text, power, toughness, colors, color_identity, rarity, keywords, image_uri, legalities, prices, json_raw
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    ON CONFLICT(id) DO UPDATE SET
        name=excluded.name,
        name_norm=excluded.name_norm,
        set_code=excluded.set_code,
        set_name=excluded.set_name,
        collector_number=excluded.collector_number,
        lang=excluded.lang,
        released_at=excluded.released_at,
        layout=excluded.layout,
        mana_cost=excluded.mana_cost,
        cmc=excluded.cmc,
        type_line=excluded.type_line,
        oracle_text=excluded.oracle_text,
        power=excluded.power,
        toughness=excluded.toughness,
        colors=excluded.colors,
        color_identity=excluded.color_identity,
        rarity=excluded.rarity,
        keywords=excluded.keywords,
        image_uri=excluded.image_uri,
        legalities=excluded.legalities,
        prices=excluded.prices,
        json_raw=excluded.json_raw
    ;
    """
    cur = conn.cursor()
    count = 0
    buf = []
    for card in cards:
        buf.append(card_row_from_json(card))
        if len(buf) >= batch_size:
            cur.executemany(sql, buf)
            conn.commit()
            count += len(buf)
            buf = []
    if buf:
        cur.executemany(sql, buf)
        conn.commit()
        count += len(buf)
    return count

def need_update(conn: sqlite3.Connection, bulk_entry: Dict[str, Any]) -> bool:
    current_updated = get_meta(conn, "scryfall_updated_at")
    updated_at = bulk_entry.get("updated_at")
    return (current_updated or "") != (updated_at or "")

def cmd_update(args: argparse.Namespace) -> None:
    db_path = args.db
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = connect_db(db_path)
    ensure_schema(conn)

    print("Fetching Scryfall bulk index...", file=sys.stderr)
    index = fetch_bulk_index()
    entry = select_default_cards_entry(index)
    download_uri = entry.get("download_uri")
    updated_at = entry.get("updated_at")
    if not download_uri:
        raise RuntimeError("Bulk entry missing download_uri")

    if not need_update(conn, entry) and not args.force:
        print(f"Database is up-to-date (updated_at={updated_at}). Use --force to reimport.", file=sys.stderr)
        return

    print(f"Downloading cards from {download_uri} ...", file=sys.stderr)
    raw = download_bulk_json(download_uri)
    print("Parsing JSON...", file=sys.stderr)
    cards = parse_cards_bytes(raw)
    print(f"Importing {len(cards)} cards...", file=sys.stderr)
    n = upsert_cards(conn, cards, batch_size=args.batch_size)
    print(f"Upserted {n} cards.", file=sys.stderr)

    set_meta(conn, "scryfall_updated_at", updated_at or "")
    set_meta(conn, "scryfall_download_uri", download_uri)
    set_meta(conn, "scryfall_last_import", dt.datetime.utcnow().isoformat() + "Z")
    print("Done.", file=sys.stderr)

def print_rows(rows: Iterable[sqlite3.Row], fields: List[str], limit: Optional[int] = None) -> None:
    count = 0
    for row in rows:
        out = " | ".join(str(row[f]) if row[f] is not None else "" for f in fields)
        print(out)
        count += 1
        if limit is not None and count >= limit:
            break

def cmd_search_name(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    name_norm = f"%{normalize_name(args.name)}%"
    cur = conn.execute("""
        SELECT name, set_code, collector_number, type_line, cmc, mana_cost, rarity
        FROM cards
        WHERE name_norm LIKE ?
        ORDER BY name, released_at DESC
        LIMIT ?
    """, (name_norm, args.limit))
    print_rows(cur, ["name", "set_code", "collector_number", "type_line", "cmc", "mana_cost", "rarity"])

def cmd_search_type(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    cur = conn.execute("""
        SELECT name, set_code, collector_number, type_line, cmc, mana_cost, rarity
        FROM cards
        WHERE type_line LIKE ?
        ORDER BY name
        LIMIT ?
    """, (f"%{args.type}%", args.limit))
    print_rows(cur, ["name", "set_code", "collector_number", "type_line", "cmc", "mana_cost", "rarity"])

def cmd_search_cmc(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    q = """
        SELECT name, set_code, collector_number, type_line, cmc, mana_cost, rarity
        FROM cards
        WHERE cmc >= ? AND cmc <= ?
        ORDER BY cmc, name
        LIMIT ?
    """
    cur = conn.execute(q, (args.min, args.max, args.limit))
    print_rows(cur, ["name", "set_code", "collector_number", "type_line", "cmc", "mana_cost", "rarity"])

def _normalize_color_id(s: str) -> str:
    # Keep only letters W U B R G in canonical order
    order = "WUBRG"
    letters = [c for c in s.upper() if c in order]
    # Remove duplicates, preserve canonical order
    seen = set()
    canonical = "".join([c for c in order if c in letters and not (c in seen or seen.add(c))])
    return canonical

def cmd_search_colors(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    target = _normalize_color_id(args.id)
    if not target:
        print("Provide a non-empty color identity like W, U, BG, WUR, etc.", file=sys.stderr)
        sys.exit(2)

    # color_identity column is a JSON array like ["W","U"]. We'll compare as a concatenated string for simplicity.
    # SQLite JSON1 is not guaranteed available, so we use string ops on the serialized array.
    # We'll store canonical like "WU" by transforming JSON to string without brackets/quotes/commas.
    cur = conn.execute("""
        SELECT name, set_code, collector_number, type_line, cmc, mana_cost, rarity, color_identity
        FROM cards
        WHERE color_identity IS NOT NULL
    """)

    rows = []
    for row in cur:
        try:
            arr = json.loads(row["color_identity"])
            canonical = "".join(c for c in "WUBRG" if c in arr)
        except Exception:
            canonical = ""
        match = (canonical == target) if not args.subset else set(target).issubset(set(canonical))
        if match:
            rows.append(row)

    # print up to limit
    fields = ["name", "set_code", "collector_number", "type_line", "cmc", "mana_cost", "rarity"]
    for row in rows[: args.limit]:
        print(" | ".join(str(row[f]) if row[f] is not None else "" for f in fields))

def cmd_card(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    # Try exact (case-insensitive) first
    name_norm = normalize_name(args.name)
    cur = conn.execute("""
        SELECT *
        FROM cards
        WHERE name_norm = ?
        ORDER BY released_at DESC
        LIMIT 1
    """, (name_norm,))
    row = cur.fetchone()
    if row is None:
        # Fallback to LIKE
        cur = conn.execute("""
            SELECT *
            FROM cards
            WHERE name_norm LIKE ?
            ORDER BY released_at DESC
            LIMIT 1
        """, (f"%{name_norm}%",))
        row = cur.fetchone()
    if row is None:
        print("No matching card found.", file=sys.stderr)
        sys.exit(1)

    # Pretty print details
    print("=" * 80)
    print(f"{row['name']} — {row['set_name']} ({row['set_code'].upper() if row['set_code'] else ''}) #{row['collector_number'] or ''}")
    print("-" * 80)
    if row["type_line"]:
        print(row["type_line"])
    if row["mana_cost"] is not None or row["cmc"] is not None:
        print(f"Mana Cost: {row['mana_cost'] or ''}   CMC: {row['cmc'] if row['cmc'] is not None else ''}")
    if row["oracle_text"]:
        print("\nText:")
        print(textwrap.fill(row["oracle_text"], width=78))
    if row["power"] or row["toughness"]:
        print(f"\nPT: {row['power']}/{row['toughness']}")
    if row["rarity"]:
        print(f"Rarity: {row['rarity']}")
    if row["released_at"]:
        print(f"Released: {row['released_at']}")
    if row["image_uri"]:
        print(f"Image: {row['image_uri']}")
    if row["legalities"]:
        try:
            legal = json.loads(row["legalities"])
            legal_str = ", ".join(f"{fmt}:{st}" for fmt, st in sorted(legal.items()))
            print(f"Legalities: {legal_str}")
        except Exception:
            pass
    print("=" * 80)

def cmd_query(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    sql = args.sql.strip().rstrip(";")
    if not re.match(r"(?is)^\s*select\b", sql):
        print("Only SELECT queries are allowed.", file=sys.stderr)
        sys.exit(2)
    cur = conn.execute(sql + " LIMIT " + str(args.limit))
    # Print header
    cols = [d[0] for d in cur.description]
    print(" | ".join(cols))
    for row in cur.fetchall():
        print(" | ".join(str(row[c]) if row[c] is not None else "" for c in cols))

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Scryfall -> SQLite MTG card database tool")
    p.add_argument("--db", default=DEFAULT_DB, help=f"Path to SQLite DB (default: {DEFAULT_DB})")
    sub = p.add_subparsers(dest="cmd", required=True)

    # update
    s = sub.add_parser("update", help="Download latest Scryfall bulk and import into DB")
    s.add_argument("--batch-size", type=int, default=BATCH_SIZE, help=f"Insert batch size (default {BATCH_SIZE})")
    s.add_argument("--force", action="store_true", help="Import even if updated_at hasn't changed")
    s.set_defaults(func=cmd_update)

    # searches
    s = sub.add_parser("search-name", help="Search by (partial) name")
    s.add_argument("--name", required=True, help="Card name (partial ok)")
    s.add_argument("--limit", type=int, default=50, help="Max rows to print")
    s.set_defaults(func=cmd_search_name)

    s = sub.add_parser("search-type", help="Search by type line (partial contains)")
    s.add_argument("--type", required=True, help="Substring to match in type_line")
    s.add_argument("--limit", type=int, default=50, help="Max rows to print")
    s.set_defaults(func=cmd_search_type)

    s = sub.add_parser("search-cmc", help="Search by CMC range")
    s.add_argument("--min", type=float, default=0, help="Minimum CMC")
    s.add_argument("--max", type=float, default=99, help="Maximum CMC")
    s.add_argument("--limit", type=int, default=50, help="Max rows to print")
    s.set_defaults(func=cmd_search_cmc)

    s = sub.add_parser("search-colors", help="Search by color identity (exact or subset)")
    s.add_argument("--id", required=True, help="Color identity like W, U, B, R, G, or combos (e.g. WU, BG)")
    s.add_argument("--subset", action="store_true", help="Match cards whose color identity includes the given letters")
    s.add_argument("--limit", type=int, default=50, help="Max rows to print")
    s.set_defaults(func=cmd_search_colors)

    s = sub.add_parser("card", help="Show detailed info for a card by name (exact or partial)")
    s.add_argument("--name", required=True, help="Card name (exact preferred; falls back to partial)")
    s.set_defaults(func=cmd_card)

    s = sub.add_parser("query", help="Run a read-only SQL SELECT against the DB")
    s.add_argument("--sql", required=True, help="SELECT statement")
    s.add_argument("--limit", type=int, default=200, help="Max rows to return")
    s.set_defaults(func=cmd_query)

    return p

def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
        return 0
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        if os.environ.get("DEBUG"):
            raise
        return 1


if __name__ == "__main__":
    sys.exit(main())

