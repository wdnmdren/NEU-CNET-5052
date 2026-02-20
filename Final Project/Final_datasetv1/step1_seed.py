# step1_seed.py
import os, time, random
from datetime import datetime, timedelta, timezone
import requests
from config import *

HEADERS = {"X-Riot-Token": API_KEY}

def unix_days_ago(days: int) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

def sleep_jitter(mult=1.0):
    time.sleep(max(0.0, BASE_SLEEP * mult + random.random() * JITTER))

def get_json(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} {url} {r.text[:200]}")
    sleep_jitter()
    return r.json()

def account_by_riot_id(game_name, tag_line):
    url = f"https://{REGION_ROUTING}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    return get_json(url)

def last_match_id(puuid, start_time):
    url = f"https://{REGION_ROUTING}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"queue": QUEUE_ID, "startTime": start_time, "start": 0, "count": 1}
    ids = get_json(url, params=params)
    return ids[0] if ids else None

def match_detail(mid):
    url = f"https://{REGION_ROUTING}.api.riotgames.com/lol/match/v5/matches/{mid}"
    return get_json(url)

def teammates_of_seed(md, seed_puuid):
    info = md["info"]
    parts = info["participants"]
    seed_team = None
    for p in parts:
        if p["puuid"] == seed_puuid:
            seed_team = p["teamId"]
            break
    mates = []
    for p in parts:
        if p["teamId"] == seed_team and p["puuid"] != seed_puuid:
            mates.append(p["puuid"])
    return mates

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    start_time = unix_days_ago(DAYS_BACK)

    acct = account_by_riot_id(SEED_GAME_NAME, SEED_TAG_LINE)
    seed_puuid = acct["puuid"]
    print("seed puuid:", seed_puuid)

    mid = last_match_id(seed_puuid, start_time)
    if not mid:
        raise RuntimeError("Seed has no solo/duo match in window")
    md = match_detail(mid)

    mates = teammates_of_seed(md, seed_puuid)
    print("seed last match:", mid, "teammates:", len(mates))

    # 写文件：seed、frontier
    with open(os.path.join(OUT_DIR, "seed.txt"), "w") as f:
        f.write(seed_puuid + "\n")
    with open(os.path.join(OUT_DIR, "frontier_00.txt"), "w") as f:
        for m in mates:
            f.write(m + "\n")

    # 也把 seed 的 matchId 记下来（可选）
    with open(os.path.join(OUT_DIR, "seed_last_match.txt"), "w") as f:
        f.write(mid + "\n")

    print("written:", os.path.join(OUT_DIR, "frontier_00.txt"))

if __name__ == "__main__":
    main()
