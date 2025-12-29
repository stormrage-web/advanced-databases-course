import argparse
import csv
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class EventDims:
    devices: tuple[str, ...] = ("Mobile", "Desktop", "Tablet")
    apps: tuple[str, ...] = ("Ozon App", "Ozon Web", "Partner App")
    os: tuple[str, ...] = ("Android", "iOS", "Windows", "macOS", "Linux")
    provinces: tuple[str, ...] = (
        "Moscow",
        "Saint Petersburg",
        "Novosibirsk",
        "Kazan",
        "Yekaterinburg",
        "Other",
    )


def parse_offers_limited(path: Path, limit_offers: int) -> list[int]:
    if not path.exists():
        raise FileNotFoundError(path)
    offer_ids: list[int] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                offer_ids.append(int(row["offer_id"]))
            except Exception:
                continue
            if limit_offers and len(offer_ids) >= limit_offers:
                break
    return offer_ids


def parse_offers(path: Path) -> list[int]:
    # 0 = без лимита
    return parse_offers_limited(path, limit_offers=0)


def main() -> int:
    p = argparse.ArgumentParser(
        description=(
            "Генерация синтетических RawEvent (data/raw_events.csv) для практики. "
            "Связь: ContentUnitID=offer_id. Часть offers будет без событий (coverage)."
        )
    )
    p.add_argument("--offers", default=str(Path("data") / "offers_clean.csv"))
    p.add_argument("--output", default=str(Path("data") / "raw_events.csv"))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--day", default="2025-12-01", help="День (YYYY-MM-DD) для event_time")
    p.add_argument("--coverage", type=float, default=0.7, help="Доля offers с событиями (0..1)")
    p.add_argument("--avg_events_per_offer", type=float, default=3.0, help="Среднее событий на offer (для покрытых)")
    p.add_argument(
        "--limit_offers",
        type=int,
        default=2_000_000,
        help="Сколько offers читать из offers_clean.csv. 0 = без лимита (может быть очень тяжело).",
    )
    args = p.parse_args()

    offers_path = Path(args.offers)
    out_path = Path(args.output)

    try:
        offer_ids = parse_offers_limited(offers_path, limit_offers=args.limit_offers)
    except FileNotFoundError:
        print(f"ERROR: offers file not found: {offers_path}", file=sys.stderr)
        return 2

    if not offer_ids:
        print("ERROR: no offer_ids parsed", file=sys.stderr)
        return 2

    if not (0.0 <= args.coverage <= 1.0):
        print("ERROR: coverage must be in [0..1]", file=sys.stderr)
        return 2

    rnd = random.Random(args.seed)
    dims = EventDims()

    base_date = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    covered_n = int(len(offer_ids) * args.coverage)
    covered_ids = set(rnd.sample(offer_ids, covered_n))

    def poisson(lmbd: float) -> int:
        # Simple Poisson sampler without numpy.
        # Knuth algorithm is fine for small lambdas (we keep avg small).
        L = pow(2.718281828459045, -lmbd)
        k = 0
        p_ = 1.0
        while True:
            k += 1
            p_ *= rnd.random()
            if p_ <= L:
                return k - 1

    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows_out = 0
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["event_time", "DeviceTypeName", "ApplicationName", "OSName", "ProvinceName", "ContentUnitID"]
        )

        for oid in offer_ids:
            if oid not in covered_ids:
                continue
            n_events = max(1, poisson(args.avg_events_per_offer))
            for _ in range(n_events):
                # Spread across 24h
                seconds = rnd.randint(0, 24 * 60 * 60 - 1)
                ts = base_date + timedelta(seconds=seconds)
                w.writerow(
                    [
                        ts.strftime("%Y-%m-%d %H:%M:%S"),
                        rnd.choice(dims.devices),
                        rnd.choice(dims.apps),
                        rnd.choice(dims.os),
                        rnd.choice(dims.provinces),
                        oid,
                    ]
                )
                rows_out += 1

    print(f"OK: raw_events.csv written: {out_path}")
    print(f"Offers total: {len(offer_ids)}")
    print(f"Offers with events (coverage): {covered_n} ({args.coverage:.2%})")
    print(f"Events total: {rows_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


