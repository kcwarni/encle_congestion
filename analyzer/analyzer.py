import logging
import pandas as pd
from ast import literal_eval
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN

logger = logging.getLogger(__name__)

def clustering(df, eps=0.5, min_sample=2):
    df = df.set_index("SRCMAC")
    X = df.copy()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    dbscan = DBSCAN(eps=eps, min_samples=min_sample)
    dbscan.fit(X_scaled)

    df["num_of_loc"] = (df != 0).sum(axis=1)
    df["cluster_labels"] = dbscan.labels_
    
    labels_val_cnt = df["cluster_labels"].value_counts()
    labels_val_cnt_df = pd.DataFrame(labels_val_cnt).reset_index()

    cluster_mtx_df = pd.merge(df, labels_val_cnt_df, right_on="cluster_labels", left_on="cluster_labels", how="right")

    # cluster_labels 중복값 제거
    cluster_mtx_df = cluster_mtx_df[~cluster_mtx_df["cluster_labels"].duplicated()].reset_index(drop=True)

    # cluster_labels -1값 제거
    cluster_mtx_df = cluster_mtx_df[cluster_mtx_df["cluster_labels"] != -1].reset_index(drop=True)

    # num_of_loc 값이 2 보다 작으면 제거
    cluster_mtx_df = cluster_mtx_df[cluster_mtx_df["num_of_loc"] >= 2].reset_index(drop=True)

    logger.info(f"Result Clustering Metrix : {cluster_mtx_df.shape}")
    
    return cluster_mtx_df

def metrix_to_sequence(zone_tbl, metrix_df):
    dfs = []
    for col in metrix_df.columns[1:]:
        for n_seq in range(1, len(metrix_df.columns[1:])):
            # 해당 location이 1일 경우
            seq = metrix_df[metrix_df[col] == n_seq].apply(lambda row: row[row == (n_seq + 1)].index.tolist()[0] if row[row == (n_seq + 1)].index.tolist() else None, axis=1)
            seq = seq.dropna()
            seq_df = pd.DataFrame(seq, columns=["seq_2"])
            seq_df["seq_1"] = col
            seq_df = seq_df[["seq_1", "seq_2"]]
            dfs.append(seq_df)

    mtx_seq_df = pd.concat(dfs).reset_index(drop=True)

    # coordinates
    zone_tbl["coords"] = zone_tbl["coords"].map(literal_eval)
    coords = []
    
    for _, row in mtx_seq_df.iterrows():
        seq1_coords = zone_tbl[zone_tbl["location"] == row["seq_1"]]["coords"].values[0]
        seq2_coords = zone_tbl[zone_tbl["location"] == row["seq_2"]]["coords"].values[0]
        coords.append([seq1_coords, seq2_coords])

    mtx_seq_df["coords"] = coords
    
    return mtx_seq_df