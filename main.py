"""
date : 2024-11-14
version : 1.0.0
writer : cwkang
"""
import os
import json
import logging
import argparse
from attrdictionary  import AttrDict

from utils import init_logger
from preprocessing import Preprocessing
from analyzer import clustering, metrix_to_sequence
from plot import CongestionPolt

logger = logging.getLogger(__name__)

def main():
    init_logger()

    cli_parser = argparse.ArgumentParser()
    cli_args = cli_parser.parse_args("")

    cli_args.config_dir = "config"
    cli_args.config_file = "config.json"
    
    with open(os.path.join(cli_args.config_dir, cli_args.config_file)) as f:
        args = AttrDict(json.load(f))

    gangneung_coffee_1 = Preprocessing(args)
    pre_raw_data = gangneung_coffee_1.get_preprocessed_data(raw_data="gangneung_rawData_20241025.csv", time=True)
    srcmac_unique_df = gangneung_coffee_1.calc_srcmac_unique(pre_raw_data)
    loc_people_cnt = gangneung_coffee_1.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, loc=True)
    h_loc_spenttime_mean_df, over_basetime_both_df = gangneung_coffee_1.calc_loc_people_spenttime(pre_raw_data, srcmac_unique_df, loc=True)
    srcmac_loc_seq_metrix = gangneung_coffee_1.create_srcmac_loc_sequence_metrix(over_basetime_both_df)
    # cluster_metrix = clustering(srcmac_loc_seq_metrix)
    mtx_seq_df = metrix_to_sequence(gangneung_coffee_1.zone_tbl, srcmac_loc_seq_metrix)

    # Plot
    plot = CongestionPolt(gangneung_coffee_1.zone_tbl, mtx_seq_df)
    plot.seq_to_seq_poly_line("강릉항 주차장")
    plot.seq_to_seq_poly_line("안내소2")

if __name__ == "__main__":
    main()