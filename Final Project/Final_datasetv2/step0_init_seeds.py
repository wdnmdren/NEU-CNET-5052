import os
import pandas as pd
from config import OUT_DIR

# 手动填入 seed puuids
SEED_PUUIDS = [
    "VYihYGiLrDYPxtgG0wEQIm2wXSu3HRf3T932T4Q9CAE9_rHKXbT5lt9mmadWom9HQ8Q7qhvq0QcDZg"
]

os.makedirs(OUT_DIR, exist_ok=True)

df = pd.DataFrame({"puuid": SEED_PUUIDS})
df.to_csv(os.path.join(OUT_DIR, "anchors_depth0.csv"), index=False)
print("[done] wrote anchors_depth0.csv", len(df))
