import os, sys, time, random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Any, Set

import requests
import pandas as pd
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError

from config import *

if not API_KEY:
    raise RuntimeError("Missing RIOT_API_KEY (export RIOT_API_KEY=RGAPI-...)")

HEADERS = {"X-Riot-Token": API_KEY}

# -------- utils --------
def unix_days_ago(days: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

def sleep_jitter(mult=1.0):
    time.sleep(max(0.0, BASE_SLEEP * mult + random.random() * JITTER))

class RetryableHTTP(RuntimeError):
    pass

def robust_get_json(session: requests.Session, url: str, params=None, max_retries: int = 6) -> Any:
    backoff = 0.6
    for attempt in range(max_retries):
        try:
            r = session.get(url, headers=HEADERS, params=params, timeout=(6, 20))
            if r.status_code == 200:
                sleep_jitter()
                return r.json()

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra else (1.0 + attempt * 0.6)
                time.sleep(wait)
                continue

            if 500 <= r.status_code < 600:
                time.sleep(min(12.0, backoff + random.random()))
                backoff = min(20.0, backoff * 1.6)
                continue

            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")

        except (ConnectionError, Timeout, ChunkedEncodingError) as e:
            time.sleep(backoff)
            backoff = min(20.0, backoff * 1.7)
            if attempt == max_retries - 1:
                raise RetryableHTTP(str(e))
            try:
                session.close()
            except Exception:
                pass
            session = requests.Session()
    raise RetryableHTTP("max retries exceeded")

# -------- riot api --------
def match_ids_by_puuid(session: requests.Session, puuid: str, start_time_unix: int, count: int) -> List[str]:
    url = f"https://{REGION_ROUTING}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"queue": QUEUE_ID, "startTime": start_time_unix, "start": 0, "count": min(count, 100)}
    mids = robust_get_json(session, url, params=params)
    return mids or []

def match_detail(session: requests.Session, match_id: str) -> dict:
    url = f"https://{REGION_ROUTING}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    return robust_get_json(session, url)

# -------- parse: only anchor->teammates --------
def teammates_in_match(md: dict, anchor_puuid: str):
    info = md.get("info", {})
    parts = info.get("participants", [])
    if not parts:
        return None

    anchor_team = None
    anchor_win = None
    for p in parts:
        if p.get("puuid") == anchor_puuid:
            anchor_team = int(p.get("teamId"))
            anchor_win = bool(p.get("win"))
            break
    if anchor_team is None:
        return None

    mates = []
    for p in parts:
        pu = p.get("puuid")
        if not pu or pu == anchor_puuid:
            continue
        if int(p.get("teamId")) == anchor_team:
            mates.append(pu)

    gst = int(info.get("gameStartTimestamp", 0))
    return anchor_team, anchor_win, gst, mates

def sample_one_anchor(session, anchor: str, start_time_unix: int, rng: random.Random, match_cache: Dict[str, dict]):
    mids = match_ids_by_puuid(session, anchor, start_time_unix, RECENT_MATCH_K)

    occ = defaultdict(list)  # teammate -> list[(matchId, teamId, gst, win)]
    for mid in mids:
        try:
            md = match_cache.get(mid)
            if md is None:
                md = match_detail(session, mid)
                match_cache[mid] = md

            meta = md.get("metadata", {})
            match_id = meta.get("matchId", mid)

            t = teammates_in_match(md, anchor)
            if t is None:
                continue
            team_id, team_win, gst, mates = t

            for m in mates:
                occ[m].append((match_id, team_id, gst, team_win))

        except RetryableHTTP:
            continue
        except Exception:
            continue

    all_teammates = list(occ.keys())
    if not all_teammates:
        return [], []

    rng.shuffle(all_teammates)
    chosen_y = all_teammates[: min(SAMPLE_Y_TEAMMATES, len(all_teammates))]

    edges = []
    for mate in chosen_y:
        match_id, team_id, gst, team_win = rng.choice(occ[mate])
        u, v = (anchor, mate) if anchor < mate else (mate, anchor)
        edges.append({
            "u_puuid": u,
            "v_puuid": v,
            "anchor_puuid": anchor,
            "matchId": match_id,
            "queueId": QUEUE_ID,
            "gameStartTimestamp": gst,
            "teamId": team_id,
            "team_win": team_win,
        })

    # z 个扩展点：允许与 y 重复，所以独立抽样
    z = min(EXPAND_Z_ANCHORS, len(all_teammates))
    chosen_z = rng.sample(all_teammates, k=z)

    return edges, chosen_z

def main():
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python step_layer.py <depth>")

    depth = int(sys.argv[1])
    os.makedirs(OUT_DIR, exist_ok=True)

    in_path = os.path.join(OUT_DIR, f"anchors_depth{depth}.csv")
    out_edges_path = os.path.join(OUT_DIR, f"edges_depth{depth}.csv")
    out_anchors_path = os.path.join(OUT_DIR, f"anchors_depth{depth+1}.csv")

    anchors_df = pd.read_csv(in_path)
    anchors = anchors_df["puuid"].dropna().astype(str).tolist()

    # 每层预算控制
    anchors = anchors[:MAX_ANCHORS_PER_LAYER]

    session = requests.Session()
    rng = random.Random(0 + depth)
    start_time_unix = unix_days_ago(DAYS_BACK)

    match_cache: Dict[str, dict] = {}  # 仅本层缓存

    edges_all = []
    next_anchors_all = []

    for i, a in enumerate(anchors, 1):
        e, nxt = sample_one_anchor(session, a, start_time_unix, rng, match_cache)
        edges_all.extend(e)
        next_anchors_all.extend(nxt)

        if i % 200 == 0:
            print(f"[d{depth}] processed={i}/{len(anchors)} edges={len(edges_all)} next_candidates={len(next_anchors_all)} cache_matches={len(match_cache)}")

    # 写 edges（去重：anchor-u-v-match ）
    edf = pd.DataFrame(edges_all)
    if not edf.empty:
        edf = edf.drop_duplicates(subset=["anchor_puuid", "u_puuid", "v_puuid", "matchId"])
        edf.to_csv(out_edges_path, index=False)
    else:
        pd.DataFrame(columns=["u_puuid","v_puuid","anchor_puuid","matchId","queueId","gameStartTimestamp","teamId","team_win"]).to_csv(out_edges_path, index=False)

    # 写下一层 anchors（去重）
    ndf = pd.DataFrame({"puuid": next_anchors_all})
    ndf = ndf.dropna()
    ndf = ndf.drop_duplicates(subset=["puuid"])
    ndf.to_csv(out_anchors_path, index=False)

    print(f"[done d{depth}] anchors_in={len(anchors)} edges_out={len(edf)} anchors_next={len(ndf)}")
    print("wrote:", out_edges_path)
    print("wrote:", out_anchors_path)

if __name__ == "__main__":
    main()
