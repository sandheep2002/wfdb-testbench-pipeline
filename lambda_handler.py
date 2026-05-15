"""
ECG Annotation Pipeline — AWS Lambda Handler
=============================================
Input:  { "fileId": "<string>" }
Output: { "csvPath": "wfdb_files/{fileId}/{fileId}_<UTC>.csv",
          "atrPath": "wfdb_files/{fileId}/{fileId}_<UTC>.atr" }

Uploads to s3://stage-beatly-test-bench/wfdb_files/{fileId}/
"""

import csv
import logging
import os
import shutil
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import numpy as np
import wfdb
from pymongo import MongoClient

# ===== CONFIG =====
MONGO_URI     = "URI"
DB_NAME       = "beatly-test-bench"

S3_BUCKET     = "stage-beatly-test-bench"
S3_PREFIX     = "wfdb_files"

SRC_FS        = 125
DST_FS        = 244
STRIP_LEN_SRC = 1250
STRIP_LEN_DST = round(STRIP_LEN_SRC * DST_FS / SRC_FS)   # 2440
GAP_THRESHOLD = 100

RECORD_NAME   = "Lead_I"
EXTENSION     = "atr"

TIME_BASE = datetime(2000, 1, 1, 12, 0, 0)

RHYTHM_MAP = {
    "2D1": "MI",   "AF": "AFL",   "AHB": "AHB",  "AIVR": "AI",
    "AJR": "AJ",   "ATV": "ATV",  "Apr": "APR",  "DCP": "AVP",
    "EAR": "EAR",  "IPVC": "IPVC","ISVE": "ISVE","JT": "JT",
    "MVC": "MVC",  "SA": "SA",    "SVEQ": "SVEQ","SVT": "SVTA",
    "V esc beat": "VESC", "VEB": "VBIG", "VEC": "VCOUP",
    "VEQ": "VQ",   "VET": "VTRIG","VF": "VFIB",  "VPR": "VPR",
    "VS": "VS",
}

BEAT_MAP = {
    "ISVE": "PAC",
    "JEB":  "JE",
    "PJC":  "JPC",
    "V esc beat": "VE",
}

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────

_mongo_client: MongoClient | None = None


def get_client() -> MongoClient:
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(MONGO_URI)
        log.info("MongoDB connection opened")
    return _mongo_client


def close_client():
    global _mongo_client
    if _mongo_client is not None:
        _mongo_client.close()
        _mongo_client = None
        log.info("MongoDB connection closed")


# ─────────────────────────────────────────────
# STAGE 1 — MongoDB fetch
# ─────────────────────────────────────────────

def fetch_docs(file_id: str):
    db    = get_client()[DB_NAME]
    query = {"fileId": file_id, "approveFlag": True}

    beat_docs  = list(db["beatAnnotations"].find(query))
    range_docs = list(db["rangeAnnotations"].find(query))

    for d in beat_docs + range_docs:
        d.pop("_id", None)

    log.info(f"Fetched  beatAnnotations={len(beat_docs)}  rangeAnnotations={len(range_docs)}")
    return beat_docs, range_docs


# ─────────────────────────────────────────────
# STAGE 1 — Range preprocessing
# ─────────────────────────────────────────────

def split_ranges(ranges):
    normal, passthrough = [], []
    for r in ranges:
        s, e = r["startIndex"], r["endIndex"]
        if s < 0 or e < s:
            passthrough.append(deepcopy(r))
        else:
            normal.append(r)

    if len(normal) <= 1:
        return passthrough + [deepcopy(r) for r in normal]

    boundaries = sorted({r["startIndex"] for r in normal} | {r["endIndex"] for r in normal})
    result = []
    for r in normal:
        s, e = r["startIndex"], r["endIndex"]
        splits = [b for b in boundaries if s < b < e]
        if not splits:
            result.append(deepcopy(r))
        else:
            points = [s] + splits + [e]
            for i in range(len(points) - 1):
                nr = deepcopy(r)
                nr["startIndex"] = points[i]
                nr["endIndex"]   = points[i + 1]
                nr.pop("_id", None)
                result.append(nr)

    return passthrough + result


def merge_ranges(ranges):
    if not ranges:
        return []
    ranges = sorted(ranges, key=lambda r: (r["startIndex"], r["endIndex"]))
    merged = [deepcopy(ranges[0])]
    for curr in ranges[1:]:
        prev = merged[-1]
        if prev["rangeTag"] == curr["rangeTag"] and prev["endIndex"] == curr["startIndex"]:
            prev["endIndex"] = curr["endIndex"]
            prev.pop("_id", None)
        else:
            merged.append(deepcopy(curr))
    return merged


def remove_point_ranges(ranges):
    return [r for r in ranges if r["startIndex"] != r["endIndex"]]


def preprocess_ranges(range_docs):
    out = []
    for doc in range_docs:
        d = deepcopy(doc)
        r = split_ranges(d["ranges"])
        r = merge_ranges(r)
        r = remove_point_ranges(r)
        d["ranges"] = r
        out.append(d)
    log.info("Range preprocessing done")
    return out


# ─────────────────────────────────────────────
# STAGE 1 — Build rows
# ─────────────────────────────────────────────

def rescale(idx):
    return round(idx * DST_FS / SRC_FS)


def build_beat_rows(beat_docs, file_id):
    docs = [d for d in beat_docs if d.get("fileId") == file_id]
    docs.sort(key=lambda d: d.get("stripNumber", 0))
    rows = []
    for doc in docs:
        strip_no = doc.get("stripNumber", 1)
        offset   = (strip_no - 1) * STRIP_LEN_DST
        for b in doc.get("beats", []):
            rows.append((rescale(b["index"]) + offset, b["beat"]))
    rows.sort(key=lambda x: x[0])
    return rows


def build_rhythm_rows(range_docs, file_id):
    docs = [d for d in range_docs if d.get("fileId") == file_id]
    docs.sort(key=lambda d: d.get("stripNumber", 0))
    rows = []
    skipped_equal, skipped_invalid = 0, 0
    for doc in docs:
        strip_no = doc.get("stripNumber", 1)
        offset   = (strip_no - 1) * STRIP_LEN_DST
        for r in doc.get("ranges", []):
            s = rescale(r["startIndex"]) + offset
            e = rescale(r["endIndex"])   + offset
            if s == e:
                skipped_equal   += 1; continue
            if s > e:
                skipped_invalid += 1; continue
            rows.append((s, e, r["rangeTag"]))
    rows.sort(key=lambda x: (x[0], x[1]))
    if skipped_equal or skipped_invalid:
        log.warning(f"  Skipped ranges — equal={skipped_equal}  invalid={skipped_invalid}")
    return rows


def merge_rhythm_rows(rows):
    if not rows:
        return []
    merged = [{"s": rows[0][0], "e": rows[0][1], "lbl": rows[0][2]}]
    for s, e, lbl in rows[1:]:
        prev = merged[-1]
        if lbl == prev["lbl"] and (s - prev["e"]) <= GAP_THRESHOLD:
            prev["e"] = e
        else:
            merged.append({"s": s, "e": e, "lbl": lbl})
    return merged


def resolve_overlaps(rows):
    if not rows:
        return []

    rows = sorted(rows, key=lambda r: (r["s"], r["e"]))
    result = []
    pending = list(rows)

    i = 0
    while i < len(pending):
        curr = pending[i]
        i += 1

        if not result:
            result.append(dict(curr))
            continue

        prev = result[-1]

        if curr["s"] >= prev["e"]:
            result.append(dict(curr))
            continue

        if curr["lbl"] == prev["lbl"]:
            prev["e"] = max(prev["e"], curr["e"])
            continue

        resume_end = prev["e"]
        resume_lbl = prev["lbl"]
        prev["e"] = curr["s"]

        if prev["s"] == prev["e"]:
            result.pop()

        result.append(dict(curr))

        if resume_end > curr["e"]:
            tail = {"s": curr["e"], "e": resume_end, "lbl": resume_lbl}
            pending.insert(i, tail)

    result = [r for r in result if r["s"] < r["e"]]
    log.info(f"Overlap resolution: {len(rows)} → {len(result)} rhythm rows")
    return result


# ─────────────────────────────────────────────
# STAGE 1 — Write CSVs
# ─────────────────────────────────────────────

def write_beats_csv(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Beat_No", "Beat_Index", "Beat_Label"])
        for i, (idx, lbl) in enumerate(rows, 1):
            w.writerow([i, idx, lbl])
    log.info(f"  Beats   → {path}  ({len(rows)} rows)")


def write_rhythms_csv(rows, path):
    for i in range(len(rows) - 1):
        if rows[i]["e"] == rows[i + 1]["s"]:
            rows[i]["e"] -= 1

    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Rhythm_No", "Rhythm_Start_Index", "Rhythm_End_Index", "Rhythm_Label"])
        for i, r in enumerate(rows, 1):
            w.writerow([i, r["s"], r["e"], r["lbl"]])
    log.info(f"  Rhythms → {path}  ({len(rows)} rows)")


def run_stage1(file_id, work_dir):
    beat_docs, range_docs = fetch_docs(file_id)
    range_docs = preprocess_ranges(range_docs)

    beat_rows   = build_beat_rows(beat_docs, file_id)
    rhythm_rows = build_rhythm_rows(range_docs, file_id)
    log.info(f"Built    beats={len(beat_rows)}  rhythms_raw={len(rhythm_rows)}")

    merged_rhythm = merge_rhythm_rows(rhythm_rows)
    log.info(f"Merged   rhythms {len(rhythm_rows)} → {len(merged_rhythm)}")
    merged_rhythm = resolve_overlaps(merged_rhythm)

    beats_csv   = os.path.join(work_dir, "Lead_I_Beats_Annotations.csv")
    rhythms_csv = os.path.join(work_dir, "Lead_I_Rhythm_Annotations.csv")
    write_beats_csv(beat_rows, beats_csv)
    write_rhythms_csv(merged_rhythm, rhythms_csv)

    log.info("Stage 1 done")
    return beats_csv, rhythms_csv


# ─────────────────────────────────────────────
# STAGE 2 — CSVs → ATR
# ─────────────────────────────────────────────

def map_rhythm(label):
    return RHYTHM_MAP.get(label, label)


def map_beat(label):
    return BEAT_MAP.get(label, label)


def run_stage2(beats_csv, rhythms_csv, work_dir, fs=DST_FS):
    import pandas as pd

    beats   = pd.read_csv(beats_csv)
    rhythms = pd.read_csv(rhythms_csv)

    log.info(f"CSVs: beats={len(beats)}  rhythms={len(rhythms)}")

    events = []

    for _, r in rhythms.iterrows():
        idx   = int(r["Rhythm_Start_Index"])
        label = map_rhythm(str(r["Rhythm_Label"]).strip())
        events.append((idx, "+", f"({label}"))

    for _, b in beats.iterrows():
        idx = int(b["Beat_Index"])
        sym = map_beat(str(b["Beat_Label"]).strip())
        events.append((idx, sym, ""))

    events.sort(key=lambda e: (e[0], 0 if e[1] == "+" else 1))

    if not events:
        log.warning("Stage 2: no annotations — skipping ATR write")
        return False

    samples   = np.array([e[0] for e in events], dtype=np.int64)
    symbols   = [e[1] for e in events]
    aux_notes = [e[2] for e in events]
    chan      = np.zeros(len(events), dtype=np.int32)
    num       = np.zeros(len(events), dtype=np.int32)
    subtype   = np.zeros(len(events), dtype=np.int32)

    wfdb.wrann(
        record_name=RECORD_NAME,
        extension=EXTENSION,
        sample=samples,
        symbol=symbols,
        aux_note=aux_notes,
        chan=chan,
        num=num,
        subtype=subtype,
        fs=fs,
        write_dir=work_dir,
    )

    log.info(f"Stage 2 done — ATR written  ({len(events)} annotations)")
    return True


# ─────────────────────────────────────────────
# STAGE 3 — Clean ATR + summary CSV
# ─────────────────────────────────────────────

def fmt_time(sample, fs):
    t = TIME_BASE + timedelta(seconds=sample / fs)
    return t.strftime("%I:%M:%S%p")


def norm_aux(aux):
    return aux.lstrip("(").strip() if aux else aux


def run_stage3(work_dir, file_id, fs, summary_csv_path):
    record_path = os.path.join(work_dir, RECORD_NAME)
    ann = wfdb.rdann(record_path, EXTENSION)

    n    = len(ann.sample)
    keep = [True] * n

    prev_label = None
    for i in range(n):
        if ann.symbol[i] != "+":
            continue
        label = norm_aux(ann.aux_note[i])
        if label == prev_label:
            keep[i] = False
        else:
            prev_label = label

    idx_arr = np.array(keep)

    new_sample  = np.array(ann.sample)[idx_arr]
    new_symbol  = [s for s, k in zip(ann.symbol, keep) if k]
    new_subtype = np.array(ann.subtype)[idx_arr] if ann.subtype is not None else None
    new_chan    = np.array(ann.chan)[idx_arr]    if ann.chan    is not None else None
    new_num     = np.array(ann.num)[idx_arr]    if ann.num     is not None else None
    new_aux     = [norm_aux(a) for a, k in zip(ann.aux_note, keep) if k]

    wfdb.wrann(
        record_name=RECORD_NAME,
        extension=EXTENSION,
        sample=new_sample,
        symbol=new_symbol,
        subtype=new_subtype,
        chan=new_chan,
        num=new_num,
        aux_note=new_aux,
        fs=ann.fs,
        write_dir=work_dir,
    )

    log.info(f"Stage 3: ATR cleaned  {n} → {len(new_sample)} annotations")

    ann2   = wfdb.rdann(record_path, EXTENSION)
    events = [
        (int(ann2.sample[i]), ann2.aux_note[i])
        for i in range(len(ann2.sample))
        if ann2.symbol[i] == "+"
    ]

    if events:
        last_sample  = int(ann2.sample[-1])
        label_ranges = defaultdict(list)
        for i, (start, label) in enumerate(events):
            end = events[i + 1][0] if i + 1 < len(events) else last_sample
            label_ranges[label].append((start, end))

        new_rows = []
        for label, ranges in label_ranges.items():
            times = ", ".join(f"{fmt_time(s, fs)}-{fmt_time(e, fs)}" for s, e in ranges)
            new_rows.append([file_id, label, len(ranges), times])

        with open(summary_csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["File id", "Arrythmia", "No of Episodes", "Time of Event occurrence"])
            w.writerows(new_rows)

        log.info(f"Stage 3 done — summary CSV: {summary_csv_path}  ({len(new_rows)} arrhythmia rows)")
    else:
        log.warning("Stage 3: no rhythm (+) events — summary CSV not written")


# ─────────────────────────────────────────────
# S3 Upload
# ─────────────────────────────────────────────

def upload_to_s3(local_path: str, s3_key: str) -> str:
    s3 = boto3.client("s3")
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    log.info(f"Uploaded  s3://{S3_BUCKET}/{s3_key}")
    return s3_key


# ─────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────

def lambda_handler(event, context):
    file_id = event.get("fileId")
    if not file_id:
        raise ValueError("Missing 'fileId' in event")

    utc_str  = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    work_dir = f"/tmp/{file_id}"

    os.makedirs(work_dir, exist_ok=True)

    try:
        # Stage 1 — MongoDB → intermediate CSVs (tmp only)
        beats_csv, rhythms_csv = run_stage1(file_id, work_dir)

        # Stage 2 — CSVs → ATR
        atr_local      = os.path.join(work_dir, f"{RECORD_NAME}.{EXTENSION}")
        summary_local  = os.path.join(work_dir, "Techindia.csv")

        ok = run_stage2(beats_csv, rhythms_csv, work_dir, fs=DST_FS)

        if not ok:
            log.warning(f"No annotations for '{file_id}' — stages 2 & 3 skipped")
            return {
                "statusCode": 200,
                "fileId": file_id,
                "message": "No annotations found — nothing uploaded",
                "csvPath": None,
                "atrPath": None,
            }

        # Stage 3 — Clean ATR + summary CSV
        run_stage3(work_dir, file_id, DST_FS, summary_local)

        # S3 keys — final files
        csv_key = f"{S3_PREFIX}/{file_id}/{file_id}_{utc_str}.csv"
        atr_key = f"{S3_PREFIX}/{file_id}/{file_id}_{utc_str}.atr"

        # S3 keys — raw files
        beats_key   = f"{S3_PREFIX}/raw_files/{file_id}/Lead_I_Beats_Annotations.csv"
        rhythms_key = f"{S3_PREFIX}/raw_files/{file_id}/Lead_I_Rhythm_Annotations.csv"

        # Upload final files
        upload_to_s3(summary_local, csv_key)
        upload_to_s3(atr_local,     atr_key)

        # Upload raw files (for backtracking only)
        upload_to_s3(beats_csv,   beats_key)
        upload_to_s3(rhythms_csv, rhythms_key)

        return {
            "statusCode": 200,
            "fileId": file_id,
            "csvPath": csv_key,
            "atrPath": atr_key,
        }

    finally:
        # Clean up /tmp to avoid bleed between warm Lambda invocations
        shutil.rmtree(work_dir, ignore_errors=True)
        close_client()
