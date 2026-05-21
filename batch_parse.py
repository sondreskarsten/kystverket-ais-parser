import os, sys, json, time, subprocess
sys.path.insert(0, '/home/claude/ais')

BUCKET = "sondre_brreg_data"

exec(open('/mnt/skills/user/gcs-parquet/scripts/bootstrap.py').read())

with gcs_fs.open_input_stream(f"{BUCKET}/ais/raw/_checkpoint/statinfo_voyages/aprjun2025.json") as f:
    sv_done = set(json.loads(f.read().decode()).get('done', []))

with gcs_fs.open_input_stream(f"{BUCKET}/ais/raw/_checkpoint/positions/aprjun2025.json") as f:
    pos_done = json.loads(f.read().decode()).get('done', [])

from collections import Counter
day_hours = Counter()
for h in pos_done:
    day_hours[h.split('T')[0]] += 1
complete_days = sorted(d for d, c in day_hours.items() if c == 24)

parseable = [d for d in complete_days if d in sv_done]
print(f"parseable days (positions 24/24 + statinfo done): {len(parseable)}")

already_parsed = set()
try:
    for f, _ in list_pq("ais/silver/positions_decoded/"):
        parts = dict(p.split('=') for p in f.split('/') if '=' in p)
        if all(k in parts for k in ('year','month','day')):
            already_parsed.add(f"{parts['year']}-{parts['month']}-{parts['day'].replace('.parquet','')}")
except:
    pass
print(f"already parsed: {len(already_parsed)}")

todo = [d for d in parseable if d not in already_parsed]
print(f"to parse: {len(todo)}")

BATCH = int(os.environ.get('BATCH', '5'))
todo = todo[:BATCH]
print(f"this batch: {len(todo)} days")

for day in todo:
    print(f"\n=== {day} ===")
    yr, mn, dy = day.split('-')
    local_dir = f"/tmp/ais_parse/{day}"
    os.makedirs(f"{local_dir}/out", exist_ok=True)

    t0 = time.time()
    files = list_pq(f"ais/raw/positions/year={yr}/month={mn}/day={dy}/")
    for f, sz in files:
        local = f"{local_dir}/{f.split('/')[-1]}"
        if os.path.exists(local) and os.path.getsize(local) > 0:
            continue
        import pyarrow.parquet as pq
        t = read_pq(f)
        pq.write_table(t, local)

    for kind, path in [
        ("statinfo", f"ais/raw/statinfo/year={yr}/month={mn}/day={dy}.parquet"),
        ("voyages", f"ais/raw/voyages/year={yr}/month={mn}/day={dy}.parquet"),
    ]:
        local = f"{local_dir}/{kind}.parquet"
        if not os.path.exists(local):
            try:
                t = read_pq(path)
                import pyarrow.parquet as pq
                pq.write_table(t, local)
            except:
                pass

    fr_files = list_pq("fiskeridir/parsed/v1/state/")
    if fr_files:
        local = f"{local_dir}/fartoy.parquet"
        if not os.path.exists(local):
            import pyarrow.parquet as pq
            pq.write_table(read_pq(fr_files[-1][0]), local)

    dl_time = time.time() - t0
    print(f"  downloaded in {dl_time:.0f}s")

    t1 = time.time()
    result = subprocess.run(
        ["Rscript", "/home/claude/kystverket-ais-parser/src/parse_day.R", day],
        capture_output=True, text=True, timeout=300
    )
    r_time = time.time() - t1

    if result.returncode != 0:
        print(f"  R FAILED ({r_time:.0f}s): {result.stderr[-200:]}")
        import shutil
        shutil.rmtree(local_dir, ignore_errors=True)
        continue

    for line in result.stdout.strip().split('\n'):
        if 'rows' in line.lower() or 'done' in line.lower() or 'vessels' in line.lower():
            print(f"  {line.strip()}")

    t2 = time.time()
    out_dir = f"{local_dir}/out"
    silver_map = {
        "positions_decoded.parquet": f"ais/silver/positions_decoded/year={yr}/month={mn}/day={dy}.parquet",
        "vessel_registry.parquet": f"ais/silver/vessel_registry/{day}.parquet",
        "voyages_maru.parquet": f"ais/silver/voyages_maru/year={yr}/month={mn}/day={dy}.parquet",
        "voyages_kystverket.parquet": f"ais/silver/voyages_kystverket/{day}.parquet",
    }
    for fname, gcs_path in silver_map.items():
        local = os.path.join(out_dir, fname)
        if os.path.exists(local) and os.path.getsize(local) > 0:
            import pyarrow.parquet as pq
            t = pq.read_table(local)
            write_pq(t, gcs_path)

    up_time = time.time() - t2
    print(f"  done: dl={dl_time:.0f}s R={r_time:.0f}s up={up_time:.0f}s total={time.time()-t0:.0f}s")

    import shutil
    shutil.rmtree(local_dir, ignore_errors=True)

print(f"\nBatch complete.")
