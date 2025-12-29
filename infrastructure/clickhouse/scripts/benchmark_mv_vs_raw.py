import argparse
import statistics
import time
import urllib.parse
import urllib.request


def ch_http(query: str, url: str) -> str:
    data = query.encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "text/plain; charset=utf-8")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def time_query(query: str, url: str, runs: int, warmup: int) -> list[float]:
    # warmup
    for _ in range(warmup):
        ch_http(query, url)
    times: list[float] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        ch_http(query, url)
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000.0)
    return times


def fmt_stats(name: str, times_ms: list[float]) -> str:
    times_sorted = sorted(times_ms)
    p50 = statistics.median(times_sorted)
    p95 = times_sorted[int(0.95 * (len(times_sorted) - 1))]
    avg = statistics.mean(times_sorted)
    return f"{name}: runs={len(times_ms)} avg={avg:.2f}ms p50={p50:.2f}ms p95={p95:.2f}ms"


def main() -> int:
    p = argparse.ArgumentParser(description="Сравнение времени запросов MV/AGG vs RAW в ClickHouse.")
    p.add_argument(
        "--url",
        default="http://127.0.0.1:8123/?user=lab",
        help="ClickHouse HTTP endpoint (добавьте ?user=lab если включена аутентификация)",
    )
    p.add_argument("--runs", type=int, default=20)
    p.add_argument("--warmup", type=int, default=5)
    args = p.parse_args()

    benches = [
        (
            "Offers by category",
            """
SELECT category_id, uniqExact(seller_id) AS sellers_uniq, avg(price) AS avg_price, count() AS offers_cnt
FROM ozon_analytics.ecom_offers
GROUP BY category_id
ORDER BY offers_cnt DESC
LIMIT 200
FORMAT Null
""",
            """
SELECT
  category_id,
  uniqExactMerge(sellers_uniq_state) AS sellers_uniq,
  avgMerge(avg_price_state) AS avg_price,
  countMerge(offers_cnt_state) AS offers_cnt
FROM ozon_analytics.catalog_by_category_agg
GROUP BY category_id
ORDER BY offers_cnt DESC
LIMIT 200
FORMAT Null
""",
        ),
        (
            "Events by offer (coverage core)",
            """
SELECT ContentUnitID AS offer_id, count() AS events_cnt
FROM ozon_analytics.raw_events
GROUP BY ContentUnitID
ORDER BY events_cnt DESC
LIMIT 5000
FORMAT Null
""",
            """
SELECT offer_id, countMerge(events_cnt_state) AS events_cnt
FROM ozon_analytics.events_by_offer_agg
GROUP BY offer_id
ORDER BY events_cnt DESC
LIMIT 5000
FORMAT Null
""",
        ),
        (
            "Events by category (RAW join vs MV)",
            """
SELECT o.category_id, count() AS events_cnt, uniqExact(e.ContentUnitID) AS uniq_offers
FROM ozon_analytics.raw_events AS e
ANY INNER JOIN ozon_analytics.ecom_offers AS o
  ON o.offer_id = e.ContentUnitID
GROUP BY o.category_id
ORDER BY events_cnt DESC
LIMIT 200
FORMAT Null
""",
            """
SELECT category_id,
       countMerge(events_cnt_state) AS events_cnt,
       uniqExactMerge(uniq_offers_state) AS uniq_offers
FROM ozon_analytics.events_by_category_agg
GROUP BY category_id
ORDER BY events_cnt DESC
LIMIT 200
FORMAT Null
""",
        ),
    ]

    print("Benchmarking RAW vs MV...")
    for title, raw_q, mv_q in benches:
        raw_times = time_query(raw_q, args.url, runs=args.runs, warmup=args.warmup)
        mv_times = time_query(mv_q, args.url, runs=args.runs, warmup=args.warmup)

        print(f"\n== {title} ==")
        print(fmt_stats("RAW", raw_times))
        print(fmt_stats("MV ", mv_times))

        raw_p50 = statistics.median(raw_times)
        mv_p50 = statistics.median(mv_times)
        if mv_p50 > 0:
            print(f"Speedup (p50): {raw_p50 / mv_p50:.2f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


