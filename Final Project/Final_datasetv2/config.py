import os

API_KEY = "RGAPI-4bffca4b-ceaf-407b-aa06-f755990a1f7e"

REGION_ROUTING = "americas"
QUEUE_ID = 420
DAYS_BACK = 30

RECENT_MATCH_K = 15         # x
SAMPLE_Y_TEAMMATES = 15      # y
EXPAND_Z_ANCHORS = 5        # z

MAX_ANCHORS_PER_LAYER = 5000  # 每层最多处理多少 anchor（预算控制）
BASE_SLEEP = 0.30
JITTER = 0.20

OUT_DIR = "data"
