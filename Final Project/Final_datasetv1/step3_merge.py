# step3_merge_aggregate.py
import os
import glob
import pandas as pd

from config import OUT_DIR, QUEUE_ID

def main():
    edge_files = sorted(glob.glob(os.path.join(OUT_DIR, "edges_part_*.csv")))
    player_files = sorted(glob.glob(os.path.join(OUT_DIR, "players_part_*.csv")))

    if not edge_files:
        raise RuntimeError(f"No edges_part_*.csv found in {OUT_DIR}")

    # 1) merge edges
    dfe = pd.concat([pd.read_csv(f) for f in edge_files], ignore_index=True)
    # defensive: queue filter
    if "queueId" in dfe.columns:
        dfe = dfe[dfe["queueId"] == QUEUE_ID].copy()

    # dedup: same match same pair once
    dfe = dfe.drop_duplicates(subset=["u_puuid", "v_puuid", "matchId"])

    edges_all_path = os.path.join(OUT_DIR, "edges_all.csv")
    dfe.to_csv(edges_all_path, index=False)
    print("[saved]", edges_all_path, "rows=", len(dfe))

    # 2) aggregate undirected edges across matches
    # games: number of distinct matches together
    # wins: number of those matches they won together
    agg = (
        dfe.groupby(["u_puuid", "v_puuid"])
           .agg(
               games=("matchId", "nunique"),
               wins=("team_win", "sum"),
               first_ts=("gameStartTimestamp", "min"),
               last_ts=("gameStartTimestamp", "max"),
           )
           .reset_index()
    )
    agg["winrate"] = agg["wins"] / agg["games"]

    edges_agg_path = os.path.join(OUT_DIR, "edges_agg.csv")
    agg.to_csv(edges_agg_path, index=False)
    print("[saved]", edges_agg_path, "rows=", len(agg))

    # 3) merge players + compute per-player winrate (sample-based)
    if player_files:
        dfp = pd.concat([pd.read_csv(f) for f in player_files], ignore_index=True)
        if "queueId" in dfp.columns:
            dfp = dfp[dfp["queueId"] == QUEUE_ID].copy()

        players = (
            dfp.groupby("puuid")["win"]
               .agg(games="count", wins="sum")
               .reset_index()
        )
        players["winrate"] = players["wins"] / players["games"]
    else:
        players = pd.DataFrame(columns=["puuid", "games", "wins", "winrate"])

    players_path = os.path.join(OUT_DIR, "players.csv")
    players.to_csv(players_path, index=False)
    print("[saved]", players_path, "rows=", len(players))

if __name__ == "__main__":
    main()
