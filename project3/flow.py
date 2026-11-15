"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> two charts
(parent posts vs. replies).

  python flow.py
"""
import math
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))       # "raw" or "sample"
TAG = "" if NAME == "raw" else "_" + NAME

DB = f"bsky{TAG}.duckdb"
CHART = "charts/langs_length_{kind}" + TAG + ".png"   # kind = "parents" or "replies"


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of quakes.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]


@task
def stage():
    with duckdb.connect(DB) as con:
        # 1. The same Kafka record landed twice (at-least-once): keep one copy.
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_posts
            WHERE action <> 'malformed' AND kind = 'commit'
            AND did IS NOT NULL AND rkey IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_posts WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")
                }
        
@task
def entities():
    """Kind 2: one row per (did, rkey, operation). Rows matching on all three
    are the same event delivered twice; keep the latest copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT did, rkey, operation, lang, text,
                   make_timestamp(time_us) AS event_time,  -- microseconds -> UTC
                   (raw::JSON ->> '$.data.commit.record.reply.parent.uri')
                       IS NOT NULL AS is_reply  -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY did, rkey, operation
                                       ORDER BY time_us DESC, kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM posts").fetchone()[0]

TOP_LANGS = 12  # 4x3 grid; the long tail of languages has too few posts to histogram
BIN = 20        # characters per bin -> 15 bins across 0..300


@task
def marts():
    """Mart: post-length histogram per (is_reply, lang, bin), top languages only.
    One row per bar; chart() reads this table and only draws."""
    with duckdb.connect(DB) as con:
        con.execute(f"""
            CREATE OR REPLACE TABLE mart_lang_length AS
            WITH p AS (
                SELECT is_reply,
                       split_part(lang, '-', 1) AS lang,          -- en-US -> en
                       LEAST(length_grapheme(text), 299) AS len   -- exactly 300 -> last bin
                FROM posts
                WHERE lang IS NOT NULL AND text IS NOT NULL      -- deletes have no text
            ),
            top AS (  -- top N languages, ranked separately for parents and replies
                SELECT is_reply, lang, count(*) AS total, avg(len) AS avg_len
                FROM p
                GROUP BY is_reply, lang
                QUALIFY row_number() OVER (PARTITION BY is_reply
                                           ORDER BY count(*) DESC, lang) <= {TOP_LANGS}
            )
            SELECT is_reply, lang, total, avg_len,
                   floor(len / {BIN})::INT * {BIN} AS bin,
                   count(*) AS n
            FROM p JOIN top USING (is_reply, lang)
            GROUP BY ALL
        """)


@task
def chart(replies: bool):
    """One histogram grid from the mart: replies=True -> only replies, False -> only top-level posts."""
    kind = "replies" if replies else "parents"
    path = CHART.format(kind=kind)
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("""
            SELECT lang, total, avg_len, bin, n
            FROM mart_lang_length
            WHERE is_reply = ?
            ORDER BY total DESC, lang, bin
        """, [replies]).fetchall()

    # Pivot rows -> {lang: {"total":, "avg":, "bins": {bin_start: n}}}, keeping order by size
    langs = {}
    for lang, total, avg_len, b, n in rows:
        d = langs.setdefault(lang, {"total": total, "avg": avg_len, "bins": {}})
        d["bins"][b] = n

    # Grid sized from however many languages actually came back
    ncols = min(3, len(langs))
    nrows = math.ceil(len(langs) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(4 * ncols, 2.8 * nrows),
                             sharex=True, sharey=True, squeeze=False)
    edges = list(range(0, 300, BIN))

    for ax, (lang, d) in zip(axes.flat, langs.items()):
        # Percent of that language's posts, so 'en' (thousands) and 'ko' (dozens) are comparable
        pct = [100 * d["bins"].get(b, 0) / d["total"] for b in edges]
        ax.bar(edges, pct, width=BIN, align="edge", color="#2a7fb8", edgecolor="white")
        ax.axvline(d["avg"], color="#d1495b", linestyle="--", linewidth=1)
        ax.set_title(f"{lang}  (n={d['total']:,}, avg={d['avg']:.0f})", fontsize=10)
        ax.set_xlim(0, 300)

    for ax in axes.flat[len(langs):]:   # blank out unused cells in the last row
        ax.set_visible(False)

    fig.supxlabel("graphemes per post (max 300)")
    fig.supylabel("% of that language's posts")
    fig.suptitle(f"{'Reply' if replies else 'Parent post'} length distribution, "
                 f"top {len(langs)} languages")
    fig.tight_layout()

    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


@flow(name="bsky-pipeline")
def bsky_pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    b = entities()
    marts()
    parents_path = chart(replies=False)
    replies_path = chart(replies=True)
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique entries:            %d", b)
    log.info("Chart (parents): %s", parents_path)
    log.info("Chart (replies): %s", replies_path)


if __name__ == "__main__":
    bsky_pipeline()
