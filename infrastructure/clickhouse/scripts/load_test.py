import argparse
import concurrent.futures as cf
import random
import statistics
import time
import urllib.request


def ch_http(query: str, url: str, timeout_s: float) -> None:
    data = query.encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "text/plain; charset=utf-8")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        resp.read()  # drain body


def worker(url: str, timeout_s: float, stop_at: float, queries: list[str], rng_seed: int) -> list[float]:
    rnd = random.Random(rng_seed)
    lat_ms: list[float] = []
    while time.perf_counter() < stop_at:
        q = rnd.choice(queries)
        t0 = time.perf_counter()
        try:
            ch_http(q, url, timeout_s=timeout_s)
            t1 = time.perf_counter()
            lat_ms.append((t1 - t0) * 1000.0)
        except Exception:
            # record as timeout/error (cap)
            lat_ms.append(timeout_s * 1000.0)
    return lat_ms


def main() -> int:
    p = argparse.ArgumentParser(description="Нагрузочное тестирование ClickHouse через HTTP интерфейс.")
    p.add_argument(
        "--url",
        default="http://127.0.0.1:8123/?user=lab",
        help="ClickHouse HTTP endpoint (добавьте ?user=lab если включена аутентификация)",
    )
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--duration", type=int, default=20, help="seconds")
    p.add_argument("--timeout", type=float, default=10.0, help="seconds per request")
    args = p.parse_args()

    # Mix of queries to simulate analytics workload.
    queries = [
        "SELECT category_id, count() AS offers_cnt FROM ozon_analytics.ecom_offers GROUP BY category_id ORDER BY offers_cnt DESC LIMIT 20",
        "SELECT vendor, count() AS offers_cnt FROM ozon_analytics.ecom_offers GROUP BY vendor ORDER BY offers_cnt DESC LIMIT 30",
        "SELECT category_id, countMerge(offers_cnt_state) AS offers_cnt FROM ozon_analytics.catalog_by_category_agg GROUP BY category_id ORDER BY offers_cnt DESC LIMIT 20",
        "SELECT vendor, countMerge(offers_cnt_state) AS offers_cnt FROM ozon_analytics.catalog_by_brand_agg GROUP BY vendor ORDER BY offers_cnt DESC LIMIT 30",
        "SELECT DeviceTypeName, countMerge(events_cnt_state) AS events_cnt FROM ozon_analytics.events_by_device_agg GROUP BY DeviceTypeName ORDER BY events_cnt DESC",
    ]

    stop_at = time.perf_counter() + float(args.duration)
    print(f"Load test: concurrency={args.concurrency} duration={args.duration}s url={args.url}")

    all_lat: list[float] = []
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = [
            ex.submit(worker, args.url, args.timeout, stop_at, queries, 1000 + i)
            for i in range(args.concurrency)
        ]
        for f in cf.as_completed(futs):
            all_lat.extend(f.result())

    total = len(all_lat)
    all_lat.sort()
    p50 = statistics.median(all_lat) if all_lat else 0.0
    p95 = all_lat[int(0.95 * (total - 1))] if total else 0.0
    avg = statistics.mean(all_lat) if all_lat else 0.0
    qps = total / float(args.duration) if args.duration > 0 else 0.0

    print(f"Requests: {total}")
    print(f"QPS: {qps:.2f}")
    print(f"Latency ms: avg={avg:.2f} p50={p50:.2f} p95={p95:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


