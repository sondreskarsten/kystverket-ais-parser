"""kystverket-ais-parser Cloud Run Job entrypoint.

Downloads raw data from GCS, runs parse_day.R for each day, uploads silver outputs.

Environment variables:
  WINDOW_START  date (default 2025-04-01)
  WINDOW_END    date (default 2025-07-01)
  GCS_BUCKET    default sondre_brreg_data
"""
import os, sys, json, time, subprocess, shutil
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GAuthRequest

BUCKET = os.getenv("GCS_BUCKET", "sondre_brreg_data")
WINDOW_START = os.getenv("WINDOW_START", "2025-04-01")
WINDOW_END = os.getenv("WINDOW_END", "2025-07-01")
RSCRIPT = os.getenv("RSCRIPT_PATH", "/usr/bin/Rscript")
PARSE_SCRIPT = os.getenv("PARSE_SCRIPT", "/app/parse_day.R")
CHECKPOINT_KEY = f"{BUCKET}/ais/silver/_checkpoint/parser.json"

def gcs_fs():
    key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if key and os.path.exists(key):
        creds = service_account.Credentials.from_service_account_file(
            key, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    else:
        import google.auth
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(GAuthRequest())
    return pafs.GcsFileSystem(access_token=creds.token, credential_token_expiration=creds.expiry)

def list_parquets(fs, prefix):
    sel = pafs.FileSelector(f"{BUCKET}/{prefix}", recursive=False)
    return [fi for fi in fs.get_file_info(sel) if fi.is_file and fi.path.endswith(".parquet")]

def load_checkpoint(fs):
    try:
        with fs.open_input_stream(CHECKPOINT_KEY) as f:
            return json.loads(f.read().decode())
    except:
        return {"done": []}

def save_checkpoint(fs, cp):
    with fs.open_output_stream(CHECKPOINT_KEY) as f:
        f.write(json.dumps(cp).encode())

def all_days():
    d = datetime.strptime(WINDOW_START, "%Y-%m-%d").date()
    end = datetime.strptime(WINDOW_END, "%Y-%m-%d").date()
    while d < end:
        yield d.isoformat()
        d += timedelta(days=1)

def download_parquets(fs, prefix, local_dir):
    files = list_parquets(fs, prefix)
    for fi in files:
        local = os.path.join(local_dir, os.path.basename(fi.path))
        if os.path.exists(local) and os.path.getsize(local) > 0:
            continue
        t = pq.read_table(fs.open_input_file(fi.path))
        pq.write_table(t, local, compression="snappy")
    return len(files)

def process_day(day, fs, fartoy_path):
    yr, mn, dy = day.split("-")
    ld = f"/tmp/ais_parse/{day}"
    os.makedirs(f"{ld}/out", exist_ok=True)

    t0 = time.time()
    n_pos = download_parquets(fs, f"ais/raw/positions/year={yr}/month={mn}/day={dy}/", ld)
    if n_pos < 24:
        print(f"  {day} SKIP (only {n_pos}/24 position parquets)", flush=True)
        shutil.rmtree(ld, ignore_errors=True)
        return "skip"

    for kind, path in [
        ("statinfo", f"ais/raw/statinfo/year={yr}/month={mn}/day={dy}.parquet"),
        ("voyages", f"ais/raw/voyages/year={yr}/month={mn}/day={dy}.parquet"),
    ]:
        local = f"{ld}/{kind}.parquet"
        try:
            t = pq.read_table(fs.open_input_file(f"{BUCKET}/{path}"))
            pq.write_table(t, local, compression="snappy")
        except:
            pass

    if fartoy_path and not os.path.exists(f"{ld}/fartoy.parquet"):
        shutil.copy2(fartoy_path, f"{ld}/fartoy.parquet")

    dl = time.time() - t0

    t1 = time.time()
    r = subprocess.run(
        [RSCRIPT, PARSE_SCRIPT, day],
        capture_output=True, text=True, timeout=600,
        env={**os.environ, "GCS_BUCKET": BUCKET},
    )
    rt = time.time() - t1
    if r.returncode != 0:
        print(f"  {day} R FAILED ({rt:.0f}s): {r.stderr[-300:]}", flush=True)
        shutil.rmtree(ld, ignore_errors=True)
        return "fail"

    t2 = time.time()
    silver_map = {
        "positions_decoded.parquet": f"ais/silver/positions_decoded/year={yr}/month={mn}/day={dy}.parquet",
        "vessel_registry.parquet": f"ais/silver/vessel_registry/{day}.parquet",
        "voyages_maru.parquet": f"ais/silver/voyages_maru/year={yr}/month={mn}/day={dy}.parquet",
        "voyages_kystverket.parquet": f"ais/silver/voyages_kystverket/{day}.parquet",
    }
    for fname, gpath in silver_map.items():
        local = f"{ld}/out/{fname}"
        if os.path.exists(local) and os.path.getsize(local) > 0:
            t = pq.read_table(local)
            with fs.open_output_stream(f"{BUCKET}/{gpath}") as out:
                pq.write_table(t, out, compression="snappy")
    up = time.time() - t2

    print(f"  {day}: dl={dl:.0f}s R={rt:.0f}s up={up:.0f}s total={time.time()-t0:.0f}s", flush=True)
    shutil.rmtree(ld, ignore_errors=True)
    return "ok"

def main():
    print(f"[{datetime.now():%H:%M:%S}] parser {WINDOW_START}→{WINDOW_END}", flush=True)

    fs = gcs_fs()
    cp = load_checkpoint(fs)
    done_set = set(cp["done"])
    todo = [d for d in all_days() if d not in done_set]
    print(f"[{datetime.now():%H:%M:%S}] done={len(done_set)} todo={len(todo)}", flush=True)

    if not todo:
        print("nothing to do", flush=True)
        return 0

    fartoy_local = "/tmp/fartoy.parquet"
    if not os.path.exists(fartoy_local):
        fr_files = list_parquets(fs, "fiskeridir/parsed/v1/state/")
        if fr_files:
            latest = sorted(fr_files, key=lambda x: x.path)[-1]
            t = pq.read_table(fs.open_input_file(latest.path))
            pq.write_table(t, fartoy_local, compression="snappy")

    ok = 0
    fail = 0
    skip = 0
    last_save = time.time()

    for day in todo:
        result = process_day(day, fs, fartoy_local)
        if result == "ok":
            ok += 1
            done_set.add(day)
        elif result == "fail":
            fail += 1
        else:
            skip += 1

        if time.time() - last_save > 120:
            cp["done"] = sorted(done_set)
            save_checkpoint(fs, cp)
            last_save = time.time()

    cp["done"] = sorted(done_set)
    save_checkpoint(fs, cp)

    print(f"[{datetime.now():%H:%M:%S}] DONE ok={ok} fail={fail} skip={skip}", flush=True)
    return 0 if fail == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
