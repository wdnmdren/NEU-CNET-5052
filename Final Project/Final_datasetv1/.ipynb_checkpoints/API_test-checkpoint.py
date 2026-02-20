import os, time, requests

API_KEY = "RGAPI-3237d1c8-21cb-41d9-95e1-43c70c73b7fe"
if not API_KEY:
    raise RuntimeError("Missing RIOT_API_KEY env var")

HEADERS = {"X-Riot-Token": API_KEY}

REGION_ROUTING = "americas"
QUEUE_ID = 420

def get_json(url, params=None):
    t0 = time.time()
    r = requests.get(url, headers=HEADERS, params=params, timeout=(5, 20))
    dt = time.time() - t0
    return r, dt

def test_account_by_riot_id(game_name, tag_line):
    url = f"https://{REGION_ROUTING}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    r, dt = get_json(url)
    print("\n[account_by_riot_id]", url)
    print("status:", r.status_code, "time:", f"{dt:.3f}s")
    print("body:", r.text[:300])
    return r

def test_last_match_id(puuid, start_time):
    url = f"https://{REGION_ROUTING}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"queue": QUEUE_ID, "startTime": start_time, "start": 0, "count": 1}
    r, dt = get_json(url, params=params)
    print("\n[last_match_id]", url, "params=", params)
    print("status:", r.status_code, "time:", f"{dt:.3f}s")
    print("body:", r.text[:300])
    return r

def test_match_detail(mid):
    url = f"https://{REGION_ROUTING}.api.riotgames.com/lol/match/v5/matches/{mid}"
    r, dt = get_json(url)
    print("\n[match_detail]", url)
    print("status:", r.status_code, "time:", f"{dt:.3f}s")
    print("body:", r.text[:300])
    return r

if __name__ == "__main__":
    # 你自己的 seed（可改）
    GAME_NAME = "Mooncake"
    TAG_LINE = "0208"

    # 你报 500 的 matchId（可改/多试几个）
    MID = "NA1_5488754461"

    # startTime 用 unix 秒；这里随便给一个近30天的起点（你也可以换成自己的）
    # 例：30天前
    start_time = int(time.time()) - 30 * 24 * 3600

    # 1) 测试 account endpoint
    r1 = test_account_by_riot_id(GAME_NAME, TAG_LINE)
    puuid = None
    if r1.status_code == 200:
        puuid = r1.json().get("puuid")

    # 2) 测试 match ids endpoint
    if puuid:
        test_last_match_id(puuid, start_time)
    else:
        print("\n[skip last_match_id] no puuid (account lookup failed)")

    # 3) 测试 match detail endpoint（最可能 500）
    test_match_detail(MID)
