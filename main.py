"""
date : 2024-11-14
version : 1.0.0
writer : cwkang
"""
import os
import json
import logging
import argparse
import warnings
import pandas as pd
from attrdictionary  import AttrDict

from utils import init_logger
from preprocessing import Preprocessing
from analyzer import clustering, metrix_to_sequence
from plot import CongestionPolt, CongestionImagePolt

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

def main():
    init_logger()

    cli_parser = argparse.ArgumentParser()
    cli_args = cli_parser.parse_args("")

    cli_args.config_dir = "config"
    cli_args.config_file = "config.json"
    
    with open(os.path.join(cli_args.config_dir, cli_args.config_file), encoding="utf-8") as f:
        args = AttrDict(json.load(f))

    prep = Preprocessing(args)
    
    # SRCMAC Unique 값 추출
    pre_raw_data = prep.get_preprocessed_data(raw_data=args.raw_data, time=True)
    srcmac_unique_df = prep.calc_srcmac_unique(pre_raw_data)
    
    if args.task == "gangneung":
        # SRCMAC의 수
        people_cnt, _ = prep.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, cumsum="00:01:00", loc=False, on_time=False, is_save=False)

        # 시간대별 SRCMAC의 수
        h_people_cnt, _ = prep.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, cumsum="00:01:00", loc=False, on_time=True, is_save=False)

        # 공간별 체류인원수
        loc_srcmac_cnt, loc_people_cnt, srcmac_uni_matched_raw_data = prep.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, cumsum="00:01:00", loc=True, on_time=False, is_save=False)

        # 공간별 시간별 체류인원수
        loc_srcmac_cnt, loc_h_people_cnt, srcmac_uni_matched_raw_data = prep.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, cumsum="00:01:00", loc=True, on_time=True, is_save=False)

        # 일자별 공간별 시간대별 평균 체류시간
        loc_h_spenttime_mean_df = prep.calc_loc_people_spenttime(pre_raw_data, srcmac_unique_df, base_time=10, time_diff="00:01:00", loc=True, entry_time=True, is_save=True)
    
    elif args.task == "kme2024":
         # SRCMAC의 수
        people_cnt, _ = prep.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, cumsum="00:01:00", loc=False, on_time=False, is_save=False)

        # 시간대별 SRCMAC의 수
        h_people_cnt, _ = prep.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, cumsum="00:01:00", loc=False, on_time=True, is_save=False)

        # 공간별 체류인원수
        loc_srcmac_cnt, loc_people_cnt, srcmac_uni_matched_raw_data = prep.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, cumsum="00:01:00", loc=True, on_time=False, is_save=False)

        # 공간별 시간별 체류인원수
        loc_srcmac_cnt, loc_h_people_cnt, srcmac_uni_matched_raw_data = prep.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, cumsum="00:01:00", loc=True, on_time=True, is_save=False)

        # 일자별 공간별 시간대별 평균 체류시간
        loc_h_spenttime_mean_df = prep.calc_loc_people_spenttime(pre_raw_data, srcmac_unique_df, base_time=10, time_diff="00:01:00", loc=True, entry_time=True, is_save=True)
    
    # Location 별 SRCMAC의 수 및 평균 Spenttime
    h_loc_spenttime_mean_df, over_basetime_both_df = prep.calc_loc_people_spenttime(pre_raw_data, srcmac_unique_df, loc=True)
    srcmac_loc_seq_metrix = prep.create_srcmac_loc_sequence_metrix(over_basetime_both_df)
    
    # Plot를 위한 Matrix 데이터
    cluster_metrix = clustering(srcmac_loc_seq_metrix)
    mtx_seq_df = metrix_to_sequence(prep.zone_tbl, srcmac_loc_seq_metrix)
    
    # Plot 객체 생성
    if args.task == "gangneung":
        # min: 43, max: 2527
        plot = CongestionPolt(prep.zone_tbl, args)

        if args.is_line_plot == True:
            # Line Plot    
            # plot.src_to_dst_poly_line(mtx_seq_df, "강릉항 주차장")
            plot.seq_to_seq_flow_polyline(cluster_metrix, n_priority=50)

        if args.is_heatmap_plot == True:
            for hour in range(9, 21):
                # Heatmap
                plot.heat_map(loc_h_people_cnt, hour, is_interpolate=True, min_value=11, max_value=575)

    elif args.task == "kme2024":
        # min: 26, max: 794
        plot = CongestionImagePolt(prep.zone_tbl, args)

        if args.is_line_plot == True:
            plot.seq_to_seq_flow_polyline(cluster_metrix, n_priority=50)

        if args.is_heatmap_plot == True: 
            for hour in range(9, 17):
                plot.heat_map(loc_h_people_cnt, hour, radius=70, is_interpolate=True, min_value=20, max_value=469)

if __name__ == "__main__":
    main()