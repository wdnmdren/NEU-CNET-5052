import os, glob
import pandas as pd
from config import OUT_DIR

paths = sorted(glob.glob(os.path.join(OUT_DIR, "edges_depth*.csv")))
if not paths:
    raise RuntimeError("No edges_depth*.csv found.")

dfs = [pd.read_csv(p) for p in paths]
df = pd.concat(dfs, ignore_index=True)

# 全局去重：同一条边同一局只保留一次
df = df.drop_duplicates(subset=["u_puuid","v_puuid","matchId"])

out_all = os.path.join(OUT_DIR, "edges_star_all.csv")
df.to_csv(out_all, index=False)
print("[done] wrote", out_all, "rows=", len(df))

# 可选：聚合成 pair-level（games/wins/winrate）
agg = (
    df.groupby(["u_puuid","v_puuid"])
      .agg(games=("matchId","nunique"),
           wins=("team_win","sum"))
      .reset_index()
)
agg["winrate"] = agg["wins"] / agg["games"].clip(lower=1)

out_agg = os.path.join(OUT_DIR, "edges_star_agg.csv")
agg.to_csv(out_agg, index=False)
print("[done] wrote", out_agg, "rows=", len(agg))
