# config.py
import os

API_KEY = "RGAPI-6f134871-fcd2-4ba4-9bc7-6c633e1e2810"

SEED_GAME_NAME = "Mooncake"
SEED_TAG_LINE  = "0208"

REGION_ROUTING = "americas"
QUEUE_ID = 420
DAYS_BACK = 30

OUT_DIR = "crawl_parts"

# 扩展控制
MAX_NEW_MATES_PER_PLAYER = 2
RECENT_MATCH_K = 5

# 过滤 duo / premade
FILTER_SAME_PARTY_EDGES = True

# 稳定性
BASE_SLEEP = 0.25
JITTER = 0.20

# ✅ 就填这个
SEEN_MATCHES_FILE = "seen_matches.txt"
