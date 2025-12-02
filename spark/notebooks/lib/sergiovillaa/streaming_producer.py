import csv
import os
import time
import random
from pathlib import Path
import shutil

SOURCE_FILE = "/opt/spark/work-dir/data/final_project/streaming_viewership_data.csv"
OUTDIR = "/opt/spark/work-dir/final_project/incoming_data"
BATCH_LINES = 50            
SLEEP_MIN = 0.5
SLEEP_MAX = 1.5

Path(OUTDIR).mkdir(parents=True, exist_ok=True)

def read_source_rows(source):
    with open(source, "r", encoding="utf-8", errors="ignore") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header:
            yield header
        for row in reader:
            yield row

def main():
    rows = list(read_source_rows(SOURCE_FILE))
    if not rows:
        print("No rows found in source file. Check SOURCE_FILE path.")
        return

    header = rows[0]
    data_rows = rows[1:] if len(rows) > 1 else []

    if not data_rows:
        print("Source has header only or no data rows.")
        return

    idx = 0
    file_counter = 0
    while True:
        batch = []
        for _ in range(BATCH_LINES):
            batch.append(data_rows[idx % len(data_rows)])
            idx += 1

        # write to a temp file and move atomically to OUTDIR
        tmp_name = f".tmp_batch_{int(time.time())}_{file_counter}.csv"
        tmp_path = os.path.join(OUTDIR, tmp_name)
        final_name = f"batch_{int(time.time())}_{file_counter}.csv"
        final_path = os.path.join(OUTDIR, final_name)

        with open(tmp_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerows(batch)

        # atomic move
        shutil.move(tmp_path, final_path)
        print(f"Emitted {final_path} with {len(batch)} rows")

        file_counter += 1
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

if __name__ == "__main__":
    main()
