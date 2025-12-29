import argparse
import csv
import sys
from pathlib import Path


def detect_and_decode(line_bytes: bytes) -> str:
    """
    Try UTF-8 first; if it fails, fall back to CP1251.
    Replace invalid chars to guarantee ClickHouse-acceptable UTF-8 output.
    """
    try:
        return line_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return line_bytes.decode("cp1251", errors="replace")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Подготовка data/10ozon.csv -> data/offers_clean.csv (UTF-8, без индекс-колонки)."
    )
    p.add_argument("--input", default=str(Path("data") / "10ozon.csv"))
    p.add_argument("--output", default=str(Path("data") / "offers_clean.csv"))
    p.add_argument(
        "--limit",
        type=int,
        default=2_000_000,
        help="Сколько строк (offers) выгрузить (для ускорения демо). 0 = без лимита.",
    )
    args = p.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    if not in_path.exists():
        print(f"ERROR: input not found: {in_path}", file=sys.stderr)
        return 2

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Read as bytes -> decode line-by-line to be robust to mixed encodings.
    with in_path.open("rb") as f_in, out_path.open("w", encoding="utf-8", newline="") as f_out:
        # Decode first line (header)
        header_line = detect_and_decode(f_in.readline()).lstrip("\ufeff").strip("\r\n")
        if not header_line:
            print("ERROR: empty input file", file=sys.stderr)
            return 2

        # Input header has an extra first column (index). Example:
        # ,offer_id,price,seller_id,category_id,vendor
        header = next(csv.reader([header_line]))
        if len(header) >= 6 and header[0] == "":
            header = header[1:]

        expected = ["offer_id", "price", "seller_id", "category_id", "vendor"]
        if header != expected:
            print(f"ERROR: unexpected header: {header}", file=sys.stderr)
            print(f"Expected: {expected}", file=sys.stderr)
            return 2

        writer = csv.writer(f_out)
        writer.writerow(expected)

        rows_in = 0
        rows_out = 0

        for line_b in f_in:
            line = detect_and_decode(line_b).strip("\r\n")
            if not line:
                continue
            row = next(csv.reader([line]))
            rows_in += 1
            if len(row) >= 6:
                row = row[1:6]  # drop index column, keep 5 columns
            if len(row) != 5:
                continue
            writer.writerow(row)
            rows_out += 1
            if args.limit and rows_out >= args.limit:
                break

    print(f"OK: offers_clean.csv written: {out_path}")
    print(f"Rows in (data lines): {rows_in}")
    print(f"Rows out: {rows_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


