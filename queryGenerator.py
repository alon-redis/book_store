"""
queryGenerator.py
-----------------

High-throughput Redis search query generator for the ``idx:books`` index
defined in ``bookSearch.py``. It repeatedly fires a mix of "complex"
``FT.SEARCH`` and ``FT.AGGREGATE`` queries against a target Redis
instance using a configurable number of worker threads / connections,
for a configurable number of total queries and/or wall-clock duration.

Usage examples
--------------

    # Run for 60 seconds against localhost with 100 connections:
    python queryGenerator.py --host 127.0.0.1 --port 6379 \
                             --connections 100 --duration 60

    # Run exactly 1,000,000 queries with 200 connections:
    python queryGenerator.py --host redis.example.com --port 6379 \
                             --connections 200 --total-queries 1000000

    # Stop on whichever limit hits first:
    python queryGenerator.py --connections 50 --duration 30 \
                             --total-queries 500000

    # Test mode: run each query once and print results:
    python queryGenerator.py --host 127.0.0.1 --port 6379 --test-mode

The script aims for maximum throughput:
  * one shared :class:`redis.ConnectionPool` sized to ``--connections``
  * a :class:`ThreadPoolExecutor` with ``--connections`` workers, each
    holding its own :class:`redis.Redis` client (all backed by the pool)
  * pre-built query factories; no per-iteration object churn beyond the
    randomised inputs
  * lock-free per-worker counters, merged at the end
  * a light background thread that prints live QPS without blocking
    workers
"""

from __future__ import annotations

import argparse
import random
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import redis
from faker import Faker

import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search.query import NumericFilter, Query


INDEX_NAME = "idx:books"

fake = Faker()


# ---------------------------------------------------------------------------
# Query factories
# ---------------------------------------------------------------------------
#
# Each factory returns a callable that, given a :class:`redis.Redis` client,
# executes one query against Redis and returns whatever Redis returns.
# Factories are used instead of pre-built :class:`Query` objects so every
# call can use fresh random inputs without the workers sharing mutable
# state.
# ---------------------------------------------------------------------------


def q_faceted_fuzzy_search() -> Callable[[redis.Redis], object]:
    """Highly-rated fantasy/sci-fi with fuzzy+phrase text matching."""

    def run(r: redis.Redis):
        word = fake.word()
        q = (
            Query(
                f"((@title|description:(%{word}% | \"ancient kingdom\"=>"
                f"{{$slop:2; $inorder:true}})) => {{$weight:2.0}}) "
                "(@genres:{fantasy|science\\ fiction}) "
                "(@format:{hardcover|ebook}) "
                "(@is_available:{True}) "
                "(@year_published:[(1990 +inf]) "
                "(@score:[4 +inf]) "
                "(@price:[-inf (50]) "
                "-@author:\"Alon Shmuely\""
            )
            .return_fields(
                "title", "author", "score", "price",
                "year_published", "genres", "description",
            )
            .summarize(fields=["description"], context_len=15,
                       num_frags=2, sep=" ... ")
            .highlight(fields=["title", "description"], tags=("<b>", "</b>"))
            .scorer("BM25")
            .with_scores()
            .sort_by("score", asc=False)
            .paging(0, 25)
            .dialect(2)
        )
        q.add_filter(NumericFilter("rating_votes", 200, NumericFilter.INF))
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_multi_tag_geo_search() -> Callable[[redis.Redis], object]:
    """Multi-tag intersection + geo + multi-field sort."""

    def run(r: redis.Redis):
        q = (
            Query(
                "(@genres:{mystery}) (@genres:{thriller}) "
                "(@editions:{english}) (@editions:{french}) "
                "(@status:{for_sale}) "
                "(@word_count:[80000 150000]) "
                "(@chapter_count:[15 40]) "
                "(@weight_grams:[(0 +inf]) "
                "(@geo:[-0.1276 51.5074 500 km])"
            )
            .return_fields("title", "author", "score",
                           "year_published", "format")
            .sort_by("score", asc=False)
            .paging(0, 30)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_optional_boost_search() -> Callable[[redis.Redis], object]:
    """Optional clauses that only boost ranking."""

    def run(r: redis.Redis):
        q = (
            Query(
                "(@format:{paperback}) (@is_available:{True}) "
                "(@price:[-inf 30]) "
                "~(@description:love | @main_character:Emma) "
                "~@year_published:[2015 +inf]"
            )
            .return_fields("title", "main_character",
                           "year_published", "price")
            .scorer("TFIDF.DOCNORM")
            .with_scores()
            .paging(0, 50)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_geo_radius_sorted() -> Callable[[redis.Redis], object]:
    """Simple geo-radius query, sorted by distance surrogate."""

    lons_lats = [
        (-73.9857, 40.7484),   # NYC
        (-0.1276, 51.5074),    # London
        (2.3522, 48.8566),     # Paris
        (139.6917, 35.6895),   # Tokyo
        (34.7818, 32.0853),    # Tel Aviv
    ]

    def run(r: redis.Redis):
        lon, lat = random.choice(lons_lats)
        q = (
            Query(f"(@is_available:{{True}}) (@geo:[{lon} {lat} 1500 km])")
            .return_fields("title", "author", "price", "score")
            .sort_by("score", asc=False)
            .paging(0, 100)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_agg_author_productivity() -> Callable[[redis.Redis], object]:
    """Per-author productivity with QUANTILE / STDDEV reducers."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("*")
            .load("@author")
            .apply(sales_per_page="@global_sales / @pages")
            .group_by(
                ["@author"],
                reducers.count().alias("book_count"),
                reducers.avg("@score").alias("avg_score"),
                reducers.quantile("@price", 0.5).alias("median_price"),
                reducers.quantile("@global_sales", 0.95).alias("p95_sales"),
                reducers.stddev("@word_count").alias("wc_stddev"),
                reducers.avg("@sales_per_page").alias("avg_sales_per_page"),
            )
            .filter("@book_count >= 3 && @avg_score >= 3.5")
            .sort_by(aggregations.Desc("@p95_sales"))
            .limit(0, 100)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_geo_distance_buckets() -> Callable[[redis.Redis], object]:
    """Geo-distance bucketed aggregate with TOLIST / FIRST_VALUE."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest(
                "(@genres:{fantasy}) (@year_published:[2000 +inf])"
            )
            .load("@title", "@format", "@global_sales", "@geo")
            .apply(dist_m="geodistance(@geo, -73.9857, 40.7484)")
            .apply(dist_bucket_km="floor(@dist_m/100000)*100")
            .apply(fmt="upper(@format)")
            .group_by(
                ["@dist_bucket_km", "@fmt"],
                reducers.count().alias("books"),
                reducers.tolist("@title").alias("sample_titles"),
                reducers.first_value("@title",
                                     aggregations.Desc("@global_sales"))
                        .alias("bestseller"),
                reducers.max("@global_sales").alias("top_sales"),
            )
            .apply(
                headline="format(\"%s (%d sold)\", @bestseller, @top_sales)"
            )
            .sort_by(
                aggregations.Asc("@dist_bucket_km"),
                aggregations.Desc("@books"),
            )
            .limit(0, 40)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_publisher_leaderboard() -> Callable[[redis.Redis], object]:
    """Two-stage GROUPBY with top-publisher share per decade."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@is_available:{True})")
            .load("@publisher", "@global_sales", "@year_published")
            .apply(decade="floor(@year_published/10)*10")
            .group_by(
                ["@decade", "@publisher"],
                reducers.sum("@global_sales").alias("pub_sales"),
                reducers.count().alias("pub_books"),
            )
            .group_by(
                ["@decade"],
                reducers.sum("@pub_sales").alias("decade_sales"),
                reducers.first_value("@publisher",
                                     aggregations.Desc("@pub_sales"))
                        .alias("top_publisher"),
                reducers.max("@pub_sales").alias("top_publisher_sales"),
            )
            .apply(top_share="@top_publisher_sales / @decade_sales")
            .filter("@top_share > 0.01")
            .sort_by(aggregations.Asc("@decade"))
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_reading_efficiency() -> Callable[[redis.Redis], object]:
    """Reading-efficiency buckets with QUANTILE + RANDOM_SAMPLE."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest(
                "(@reading_time_minutes:[(0 +inf]) "
                "(@publishing_delay:[0 +inf])"
            )
            .load("@title", "@pages", "@chapter_count",
                  "@price", "@word_count")
            .apply(wpm="@word_count / @reading_time_minutes")
            .apply(wpm_bucket="floor( log(@wpm + 1) )")
            .group_by(
                ["@wpm_bucket"],
                reducers.count().alias("books"),
                reducers.avg("@pages").alias("avg_pages"),
                reducers.avg("@chapter_count").alias("avg_chapters"),
                reducers.quantile("@price", 0.5).alias("median_price"),
                reducers.quantile("@wpm", 0.9).alias("p90_wpm"),
                reducers.random_sample("@title", 5).alias("sample_titles"),
            )
            .sort_by(aggregations.Asc("@wpm_bucket"))
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_parameterized_price_window() -> Callable[[redis.Redis], object]:
    """Parameterized FT.SEARCH with $params binding and DIALECT 2."""

    def run(r: redis.Redis):
        lo = random.randint(1900, 1990)
        hi = lo + random.randint(5, 40)
        min_score = round(random.uniform(2.5, 4.5), 2)
        max_price = random.choice([20, 35, 50, 75, 100])
        q = (
            Query(
                "(@year_published:[$lo $hi]) "
                "(@score:[$min_score +inf]) "
                "(@price:[-inf ($max_price]) "
                "(@is_available:{True})"
            )
            .return_fields("title", "author", "year_published",
                           "score", "price")
            .sort_by("year_published", asc=True)
            .paging(0, 50)
            .dialect(2)
        )
        params = {
            "lo": lo,
            "hi": hi,
            "min_score": min_score,
            "max_price": max_price,
        }
        return r.ft(INDEX_NAME).search(q, query_params=params).docs

    return run


def q_prefix_suffix_infix_text() -> Callable[[redis.Redis], object]:
    """Prefix / suffix / infix wildcard TEXT matching with VERBATIM."""

    def run(r: redis.Redis):
        prefix = fake.lexify(text="???").lower()
        infix = fake.lexify(text="??").lower()
        suffix = fake.lexify(text="??").lower()
        q = (
            Query(
                f"(@title:{prefix}*) "
                f"(@description:*{infix}*) "
                f"(@author:*{suffix}) "
                f"(@is_available:{{True}})"
            )
            .verbatim()
            .return_fields("title", "author", "description")
            .paging(0, 20)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_negative_heavy_search() -> Callable[[redis.Redis], object]:
    """Negation-heavy query: exclude genres, statuses, formats, keywords."""

    def run(r: redis.Redis):
        q = (
            Query(
                "(@is_available:{True}) "
                "-(@genres:{horror|true\\ crime}) "
                "-(@status:{maintenance|on_loan}) "
                "-(@format:{ebook}) "
                "-(@description:(violence|gore)) "
                "-@author:\"Alon Shmuely\" "
                "(@rating_votes:[50 +inf]) "
                "(@score:[3 +inf])"
            )
            .return_fields("title", "author", "genres",
                           "format", "score", "rating_votes")
            .sort_by("rating_votes", asc=False)
            .paging(0, 40)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).search(q).docs

    return run


def q_agg_author_distinct_genres() -> Callable[[redis.Redis], object]:
    """Author versatility: distinct genres/editions + date-formatted activity."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("(@is_available:{True})")
            .load("@author", "@timestamp")
            .apply(last_active="timefmt(@timestamp, \"%Y-%m\")")
            .group_by(
                ["@author"],
                reducers.count().alias("books"),
                reducers.count_distinct("@genres").alias("distinct_genres"),
                reducers.count_distinctish("@editions")
                        .alias("distinct_editions_approx"),
                reducers.max("@timestamp").alias("last_ts"),
                reducers.tolist("@last_active").alias("active_months"),
                reducers.avg("@score").alias("avg_score"),
            )
            .apply(
                last_active_str="timefmt(@last_ts, \"%Y-%m-%d\")"
            )
            .filter("@books >= 2 && @distinct_genres >= 2")
            .sort_by(aggregations.Desc("@distinct_genres"),
                     aggregations.Desc("@books"))
            .limit(0, 50)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_price_per_page_leaders() -> Callable[[redis.Redis], object]:
    """Value-for-money leaderboard: price/page, citation/review, size vs weight."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest(
                "(@is_available:{True}) (@pages:[(0 +inf]) "
                "(@review_count:[(0 +inf])"
            )
            .load("@title", "@author", "@price", "@pages",
                  "@citation_count", "@review_count", "@weight_grams",
                  "@width_cm", "@height_cm", "@depth_cm")
            .apply(price_per_page="@price / @pages")
            .apply(citation_ratio="@citation_count / @review_count")
            .apply(volume_cm3="@width_cm * @height_cm * @depth_cm")
            .apply(density_g_cm3="@weight_grams / @volume_cm3")
            .filter("@price_per_page > 0 && @volume_cm3 > 0")
            .group_by(
                ["@author"],
                reducers.count().alias("books"),
                reducers.avg("@price_per_page").alias("avg_ppp"),
                reducers.quantile("@price_per_page", 0.5)
                        .alias("median_ppp"),
                reducers.avg("@citation_ratio").alias("avg_citation_ratio"),
                reducers.avg("@density_g_cm3").alias("avg_density"),
                reducers.first_value(
                    "@title", aggregations.Asc("@price_per_page")
                ).alias("cheapest_title"),
            )
            .filter("@books >= 2")
            .sort_by(aggregations.Asc("@median_ppp"))
            .limit(0, 25)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


def q_agg_inventory_status_funnel() -> Callable[[redis.Redis], object]:
    """Inventory funnel per format: counts per status with string formatting."""

    def run(r: redis.Redis):
        req = (
            AggregateRequest("*")
            .load("@format", "@status", "@price", "@title")
            .apply(fmt_up="upper(@format)")
            .apply(status_lc="lower(@status)")
            .group_by(
                ["@fmt_up", "@status_lc"],
                reducers.count().alias("count"),
                reducers.avg("@price").alias("avg_price"),
                reducers.min("@price").alias("min_price"),
                reducers.max("@price").alias("max_price"),
                reducers.random_sample("@title", 3).alias("samples"),
            )
            .apply(
                summary="format("
                        "\"%s/%s: %d books, avg $%.2f\", "
                        "@fmt_up, @status_lc, @count, @avg_price)"
            )
            .sort_by(aggregations.Asc("@fmt_up"),
                     aggregations.Desc("@count"))
            .limit(0, 40)
            .dialect(2)
        )
        return r.ft(INDEX_NAME).aggregate(req).rows

    return run


# The full pool of query factories the generator will cycle through.
ALL_QUERIES: List[Callable[[], Callable[[redis.Redis], object]]] = [
    q_faceted_fuzzy_search,
    q_multi_tag_geo_search,
    q_optional_boost_search,
    q_geo_radius_sorted,
    q_parameterized_price_window,
    q_prefix_suffix_infix_text,
    q_negative_heavy_search,
    q_agg_author_productivity,
    q_agg_geo_distance_buckets,
    q_agg_publisher_leaderboard,
    q_agg_reading_efficiency,
    q_agg_author_distinct_genres,
    q_agg_price_per_page_leaders,
    q_agg_inventory_status_funnel,
]


# ---------------------------------------------------------------------------
# Worker / stats plumbing
# ---------------------------------------------------------------------------


@dataclass
class WorkerStats:
    """Per-worker counters. Kept lock-free; merged at the end."""

    queries: int = 0
    errors: int = 0
    by_query: Dict[str, int] = field(default_factory=dict)
    errors_by_type: Dict[str, int] = field(default_factory=dict)


class GlobalState:
    """Shared coordination state used by all workers and the reporter."""

    def __init__(self, total_queries: Optional[int]) -> None:
        self.total_queries = total_queries
        self.stop = threading.Event()
        self._counter_lock = threading.Lock()
        self.global_queries = 0
        self.global_errors = 0

    def record(self, queries: int, errors: int) -> None:
        with self._counter_lock:
            self.global_queries += queries
            self.global_errors += errors

    def should_stop(self) -> bool:
        if self.stop.is_set():
            return True
        if self.total_queries is not None:
            with self._counter_lock:
                if self.global_queries >= self.total_queries:
                    self.stop.set()
                    return True
        return False


def worker_loop(
    pool: redis.ConnectionPool,
    state: GlobalState,
    queries: List[Callable[[], Callable[[redis.Redis], object]]],
    flush_every: int = 64,
) -> WorkerStats:
    """Run queries in a tight loop until ``state`` says to stop.

    Each worker flushes its local counts into the global counters every
    ``flush_every`` queries so the live reporter stays reasonably fresh
    without paying a lock cost on every single call.
    """

    r = redis.Redis(connection_pool=pool)
    stats = WorkerStats()

    local_q = 0
    local_e = 0

    # Pre-resolve the name list once.
    names = [f.__name__ for f in queries]
    factories = queries

    while not state.should_stop():
        idx = random.randrange(len(factories))
        name = names[idx]
        op = factories[idx]()
        try:
            op(r)
            stats.queries += 1
            stats.by_query[name] = stats.by_query.get(name, 0) + 1
            local_q += 1
        except Exception as e:  # noqa: BLE001 - we want to keep going
            stats.errors += 1
            err_key = type(e).__name__
            stats.errors_by_type[err_key] = (
                stats.errors_by_type.get(err_key, 0) + 1
            )
            local_e += 1

        if local_q + local_e >= flush_every:
            state.record(local_q, local_e)
            local_q = 0
            local_e = 0

    if local_q or local_e:
        state.record(local_q, local_e)

    return stats


def live_reporter(state: GlobalState, start_time: float,
                  interval: float = 1.0) -> None:
    """Background thread that prints a one-line live throughput report."""

    last_q = 0
    last_t = start_time

    while not state.stop.is_set():
        time.sleep(interval)
        now = time.time()
        with state._counter_lock:
            q = state.global_queries
            e = state.global_errors
        elapsed = now - start_time
        dq = q - last_q
        dt = now - last_t
        inst_qps = dq / dt if dt > 0 else 0.0
        avg_qps = q / elapsed if elapsed > 0 else 0.0
        last_q = q
        last_t = now
        sys.stdout.write(
            f"\r[t+{elapsed:6.1f}s] queries={q:>10d} errors={e:>7d} "
            f"| inst={inst_qps:>9.1f} q/s avg={avg_qps:>9.1f} q/s"
        )
        sys.stdout.flush()


def merge_stats(per_worker: List[WorkerStats]) -> WorkerStats:
    total = WorkerStats()
    for s in per_worker:
        total.queries += s.queries
        total.errors += s.errors
        for k, v in s.by_query.items():
            total.by_query[k] = total.by_query.get(k, 0) + v
        for k, v in s.errors_by_type.items():
            total.errors_by_type[k] = total.errors_by_type.get(k, 0) + v
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="High-throughput Redis search query generator for "
                    f"the '{INDEX_NAME}' index."
    )
    conn = p.add_argument_group("Connection")
    conn.add_argument("--host", default="localhost",
                      help="Redis host (default: localhost)")
    conn.add_argument("--port", type=int, default=6379,
                      help="Redis port (default: 6379)")
    conn.add_argument("--password", default=None,
                      help="Redis password, if any.")
    conn.add_argument("--db", type=int, default=0,
                      help="Redis DB index (default: 0)")
    conn.add_argument("--redis-url", default=None,
                      help="Full redis:// URL. Overrides --host/--port/"
                           "--password/--db if provided.")
    conn.add_argument("--connections", "-c", type=int, default=50,
                      help="Number of concurrent connections / workers "
                           "(default: 50). The script uses one worker "
                           "thread per connection for maximum throughput.")

    load = p.add_argument_group("Load profile (at least one required)")
    load.add_argument("--total-queries", "-n", type=int, default=None,
                      help="Total number of queries to execute across all "
                           "workers. Stops when reached.")
    load.add_argument("--duration", "-d", type=float, default=None,
                      help="Wall-clock test duration in seconds. Stops "
                           "when reached.")
    load.add_argument("--report-interval", type=float, default=1.0,
                      help="Live status refresh interval in seconds "
                           "(default: 1.0). Use 0 to disable.")

    test = p.add_argument_group("Test mode")
    test.add_argument("--test-mode", action="store_true",
                      help="Run each query exactly once, sequentially, "
                           "and print its results. Ignores --connections "
                           "/ --total-queries / --duration.")
    test.add_argument("--test-max-rows", type=int, default=5,
                      help="In --test-mode, max rows/docs to print per "
                           "query (default: 5). Use 0 for unlimited.")

    return p.parse_args(argv)


def build_pool(args: argparse.Namespace) -> redis.ConnectionPool:
    if args.redis_url:
        return redis.ConnectionPool.from_url(
            args.redis_url, max_connections=args.connections
        )
    return redis.ConnectionPool(
        host=args.host,
        port=args.port,
        db=args.db,
        password=args.password,
        max_connections=args.connections,
    )


def print_summary(total: WorkerStats, elapsed: float,
                  connections: int) -> None:
    qps = total.queries / elapsed if elapsed > 0 else 0.0
    print("\n\n=== Run summary ===")
    print(f"Elapsed time       : {elapsed:.2f} s")
    print(f"Connections/workers: {connections}")
    print(f"Total queries      : {total.queries}")
    print(f"Total errors       : {total.errors}")
    print(f"Throughput         : {qps:.1f} q/s")

    if total.by_query:
        print("\nPer-query counts:")
        width = max(len(k) for k in total.by_query)
        for name in sorted(total.by_query):
            count = total.by_query[name]
            share = count / total.queries * 100 if total.queries else 0
            print(f"  {name:<{width}}  {count:>10d}  ({share:5.1f}%)")

    if total.errors_by_type:
        print("\nErrors by type:")
        for name, count in sorted(
            total.errors_by_type.items(), key=lambda kv: -kv[1]
        ):
            print(f"  {name:<30s} {count:>10d}")


def _fmt_value(v, max_len: int = 200) -> str:
    """Pretty-format an individual cell value for test-mode output."""

    if isinstance(v, bytes):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            v = repr(v)
    s = str(v)
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def _print_test_result(name: str, doc_help: Optional[str],
                       result, max_rows: int) -> None:
    """Render the result of a single query for --test-mode."""

    header = f"### {name}"
    if doc_help:
        header += f" -- {doc_help.strip().splitlines()[0]}"
    print("\n" + "=" * 78)
    print(header)
    print("=" * 78)

    if result is None:
        print("(no result)")
        return

    # ``result`` is either a list of Document objects (FT.SEARCH .docs) or
    # a list of aggregate rows (list/dict/tuple).
    if not isinstance(result, list):
        print(_fmt_value(result, 400))
        return

    print(f"rows returned : {len(result)}")
    if max_rows and len(result) > max_rows:
        print(f"showing first : {max_rows}")
        items = result[:max_rows]
    else:
        items = result

    for i, row in enumerate(items, 1):
        print(f"\n[{i}]")
        if hasattr(row, "__dict__"):
            payload = {
                k: v for k, v in row.__dict__.items()
                if not k.startswith("_")
            }
            for k in sorted(payload):
                print(f"  {k:<24s} = {_fmt_value(payload[k])}")
        elif isinstance(row, dict):
            for k in sorted(row):
                print(f"  {str(k):<24s} = {_fmt_value(row[k])}")
        elif isinstance(row, (list, tuple)):
            # Aggregate rows from redis-py come back as flat [k, v, k, v, ...]
            # lists; show them as key/value pairs when that shape fits.
            if len(row) % 2 == 0 and all(
                isinstance(x, (str, bytes)) for x in row[::2]
            ):
                for k, v in zip(row[::2], row[1::2]):
                    key = k.decode() if isinstance(k, bytes) else str(k)
                    print(f"  {key:<24s} = {_fmt_value(v)}")
            else:
                for j, v in enumerate(row):
                    print(f"  [{j}] {_fmt_value(v)}")
        else:
            print(f"  {_fmt_value(row)}")


def run_test_mode(pool: redis.ConnectionPool, max_rows: int) -> int:
    """Run every query factory exactly once and print the results."""

    r = redis.Redis(connection_pool=pool)
    print(f"Running {len(ALL_QUERIES)} queries once each against "
          f"index '{INDEX_NAME}' (test-mode)")

    failures = 0
    for factory in ALL_QUERIES:
        name = factory.__name__
        doc = factory.__doc__
        op = factory()
        t0 = time.perf_counter()
        try:
            result = op(r)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            _print_test_result(name, doc, result, max_rows)
            print(f"\n(elapsed: {elapsed_ms:.2f} ms)")
        except Exception as e:  # noqa: BLE001
            failures += 1
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            print("\n" + "=" * 78)
            print(f"### {name} -- FAILED after {elapsed_ms:.2f} ms")
            print("=" * 78)
            print(f"{type(e).__name__}: {e}")

    print("\n" + "=" * 78)
    print(f"test-mode done: {len(ALL_QUERIES) - failures} ok, "
          f"{failures} failed")
    print("=" * 78)
    return 0 if failures == 0 else 1


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if not args.test_mode and args.total_queries is None \
            and args.duration is None:
        print(
            "error: you must specify --total-queries and/or --duration "
            "(or use --test-mode)",
            file=sys.stderr,
        )
        return 2
    if args.connections <= 0:
        print("error: --connections must be > 0", file=sys.stderr)
        return 2

    pool = build_pool(args)

    # Sanity-check connectivity up front so we fail fast.
    try:
        probe = redis.Redis(connection_pool=pool)
        probe.ping()
    except redis.exceptions.RedisError as e:
        print(f"error: cannot connect to Redis: {e}", file=sys.stderr)
        return 1

    if args.test_mode:
        return run_test_mode(pool, args.test_max_rows)

    state = GlobalState(total_queries=args.total_queries)

    # Ctrl+C => graceful shutdown.
    def _sigint(_sig, _frm):
        sys.stdout.write("\n[interrupt] stopping workers...\n")
        sys.stdout.flush()
        state.stop.set()

    signal.signal(signal.SIGINT, _sigint)

    target_desc = []
    if args.total_queries is not None:
        target_desc.append(f"{args.total_queries} queries")
    if args.duration is not None:
        target_desc.append(f"{args.duration:g}s")
    print(
        f"Starting {args.connections} workers against "
        f"{args.redis_url or f'{args.host}:{args.port}'} "
        f"(stop on: {', '.join(target_desc)})"
    )

    start = time.time()

    reporter_thread: Optional[threading.Thread] = None
    if args.report_interval > 0:
        reporter_thread = threading.Thread(
            target=live_reporter,
            args=(state, start, args.report_interval),
            daemon=True,
        )
        reporter_thread.start()

    # Enforce the duration cap from the main thread so workers stay tight.
    duration_timer: Optional[threading.Timer] = None
    if args.duration is not None:
        duration_timer = threading.Timer(args.duration, state.stop.set)
        duration_timer.daemon = True
        duration_timer.start()

    per_worker: List[WorkerStats] = []
    try:
        with ThreadPoolExecutor(max_workers=args.connections) as ex:
            futures = [
                ex.submit(worker_loop, pool, state, ALL_QUERIES)
                for _ in range(args.connections)
            ]
            for fut in as_completed(futures):
                try:
                    per_worker.append(fut.result())
                except Exception as e:  # noqa: BLE001
                    print(f"\n[worker crashed] {type(e).__name__}: {e}",
                          file=sys.stderr)
    finally:
        state.stop.set()
        if duration_timer is not None:
            duration_timer.cancel()
        if reporter_thread is not None:
            reporter_thread.join(timeout=2 * args.report_interval + 1)

    elapsed = time.time() - start
    total = merge_stats(per_worker)
    print_summary(total, elapsed, args.connections)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
