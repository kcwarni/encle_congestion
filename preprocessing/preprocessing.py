import os
import json
import pytz
import logging
import pandas as pd
import argparse
from attrdictionary  import AttrDict

logger = logging.getLogger(__name__)

class Preprocessing:
    def __init__(self, args):
        self.args = args
        # self.zone_tbl = None

    @classmethod
    def load_files(cls, path, file, encoding):
        _, file_extension = os.path.splitext(os.path.join(path, file))
    
        if file_extension == ".csv":
            df = pd.read_csv(os.path.join(path, file), encoding=encoding)
        
        elif file_extension in [".xls", ".xlsx'"]:
            df = pd.read_excel(os.path.join(path, file))
        
        else:
            raise ValueError("Only CSV or Excel files are accepted.")

        logger.info(f"{file} DataFrame Shape : {df.shape}")

        return df

    def data_preprocessing(self, zone_tbl, raw_data, time) -> pd.DataFrame:
        """
            1. "MAC" 컬럼의 dtype을 int64로 변환
            2. Location(한글 지명) 컬럼 추가
            3. "TIME" 컬럼을 datatime 타입으로 변경
            4. "TIME" 컬럼을 UTC -> KST로 변환
            5. "TIME" 컬럼을 11:00 ~ 21:00 필터
            6. "TIME" 컬럼 순으로 정렬
            6. 컬럼 순서 재정렬(TIME, MAC, Location, SRCMAC, RSSI)
        """
        # "MAC" 컬럼의 dtype을 int64로 변환
        zone_tbl["MAC"] = zone_tbl["MAC"].astype(int)
        raw_data["MAC"] = raw_data["MAC"].astype(int)

        # Location 컬럼 추가
        raw_data = pd.merge(zone_tbl[["MAC", "location"]], raw_data, left_on="MAC", right_on="MAC", how="right")
        
        # "TIME" 컬럼을 datatime 타입으로 변경
        raw_data["TIME"] = pd.to_datetime(raw_data["TIME"])

        # "TIME" 컬럼을 UTC -> KST로 변환
        utc = pytz.utc
        kst = pytz.timezone("Asia/Seoul")
        raw_data["TIME_KST"] = pd.to_datetime(raw_data["TIME"].dt.tz_localize(utc).dt.tz_convert(kst).dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]) 

        if time == True:
            # "TIME" 컬럼을 11:00 ~ 21:00 필터
            start_time = pd.to_datetime("09:00:00").time()
            end_time = pd.to_datetime("21:00:00").time()
            raw_data = raw_data[(raw_data["TIME_KST"].dt.time >= start_time) & (raw_data["TIME_KST"].dt.time <= end_time)]
        
        # "TIME" 컬럼 순으로 정렬
        raw_data = raw_data.sort_values(by="TIME_KST", ascending=True)

        # 컬럼 순서 재정렬(TIME, MAC, Location, SRCMAC, RSSI)
        raw_data = raw_data[["TIME_KST", "MAC", "location", "SRCMAC", "RSSI"]].reset_index(drop=True)

        logger.info(f"The Processed DataFrame Shape : {raw_data.shape}")
        
        return raw_data
    
    def get_preprocessed_data(self, raw_data, time=False, encoding="cp949"):
        self.zone_tbl = self.load_files(self.args.path, self.args.zone_tbl, encoding=encoding)

        return self.data_preprocessing(
            self.zone_tbl,
            self.load_files(self.args.path, raw_data, encoding=encoding),
            time
        )

    def calc_srcmac_unique(self, df, cumsum="00:01:00"):
        """
            SRCMAC의 Unique 값 구하는 함수 
        """
        # 시간 차이 계산
        time_diff_df = df.groupby("SRCMAC")["TIME_KST"].agg(["min", "max"])
        time_diff_df["time_diff"] = time_diff_df["max"] - time_diff_df["min"]

        # cumsum 계산
        srcmac_unique_df = time_diff_df[time_diff_df["time_diff"] >= pd.Timedelta(cumsum)].reset_index()
        srcmac_unique_df = srcmac_unique_df[["SRCMAC", "min", "max", "time_diff"]]

        logger.info(f"SRCMAC Unique Number : {srcmac_unique_df.shape}")
        
        return srcmac_unique_df

    def calc_loc_people_counts(self, raw_data, srcmac_unique_df, cumsum="00:01:00", loc=False, on_time=False, is_save=False):
        """
            location 별
                1. 시간 순서로 정렬
                2. 시간 차이 계산
                3. 누적 시간 계산
                4. 시간 별 계산(해당 시각, 10분 누적, 1시간 누적)
        """
        # raw_data에서 srcmac_unique를 필터링
        raw_data_df = raw_data[raw_data["SRCMAC"].isin(srcmac_unique_df["SRCMAC"])]
        raw_data_df.loc[:, "on_time"] = raw_data_df["TIME_KST"].dt.hour

        if loc == True:
            if on_time == True:
                # location, srcmac 별 time diff 계산
                time_diff_df = raw_data_df.groupby(["location", "SRCMAC", "on_time"])["TIME_KST"].agg(["min", "max"])
                time_diff_df["time_diff"] = time_diff_df["max"] - time_diff_df["min"]

                # rssi mean 계산
                riss_diff_df = raw_data_df.groupby(["location", "SRCMAC", "on_time"])["RSSI"].agg(["mean"]).astype(int)

                # time diff & rssi mean merge
                both_df = pd.merge(time_diff_df, riss_diff_df, left_index=True, right_index=True).reset_index()
                both_df = both_df[both_df["time_diff"] >= pd.Timedelta(cumsum)].reset_index(drop=True)
                both_df.columns = ["location", "SRCMAC", "on_time", "time_min", "time_max", "time_diff", "rssi_mean"]

                # Location 별 People Count 계산
                people_cnt_df = both_df.groupby(["location", "on_time"])[["SRCMAC"]].nunique().reset_index()

                if is_save == True:
                    # save 폴더가 없으면 save 폴더 생성
                    if not os.path.exists(os.path.join(self.args.path, "save")):
                        os.makedirs(os.path.join(self.args.path, "save"))
                        logger.info(f"The save folder has been created!")
                    
                    # location 별 SRCMAC 목록 데이터 저장
                    both_df.to_csv(f"{os.path.join(self.args.path, "save", self.args.save_data)}", index=False, encoding="utf-8-sig")
                    logger.info(f"{self.args.save_data} has been created!")

                    # location 별 SRCMAC의 수 데이터 저장
                    save_file = f"{self.args.save_data[:5]}_일자별 공간별 시간별 체류인원수.csv"
                    people_cnt_df.to_csv(f"{os.path.join(self.args.path, "save", save_file)}", index=False, encoding="utf-8-sig")
                    logger.info(f"{save_file} has been created!")
            
            elif on_time == False:
                # location, srcmac 별 time diff 계산
                time_diff_df = raw_data_df.groupby(["location", "SRCMAC"])["TIME_KST"].agg(["min", "max"])
                time_diff_df["time_diff"] = time_diff_df["max"] - time_diff_df["min"]

                # rssi mean 계산
                riss_diff_df = raw_data_df.groupby(["location", "SRCMAC"])["RSSI"].agg(["mean"]).astype(int)

                # time diff & rssi mean merge
                both_df = pd.merge(time_diff_df, riss_diff_df, left_index=True, right_index=True).reset_index()
                both_df = both_df[both_df["time_diff"] >= pd.Timedelta(cumsum)].reset_index(drop=True)
                both_df.columns = ["location", "SRCMAC", "time_min", "time_max", "time_diff", "rssi_mean"]

                # Location 별 People Count 계산
                people_cnt_df = both_df.groupby(["location"])[["SRCMAC"]].nunique().reset_index()

                if is_save == True:
                    # save 폴더가 없으면 save 폴더 생성
                    if not os.path.exists(os.path.join(self.args.path, "save")):
                        os.makedirs(os.path.join(self.args.path, "save"))
                        logger.info(f"The save folder has been created!")

                    # location 별 SRCMAC의 수 데이터 저장
                    save_file = f"{self.args.save_data[:5]}_일자별 공간별 체류인원수.csv"
                    people_cnt_df.to_csv(f"{os.path.join(self.args.path, "save", save_file)}", index=False, encoding="utf-8-sig")
                    logger.info(f"{save_file} has been created!")
            
            logger.info(f"People Count by Location : {both_df.shape}")
            return both_df, people_cnt_df, raw_data_df
        
        else:
            if on_time == True:
                # rssi mean 계산
                riss_diff_df = raw_data_df.groupby(["SRCMAC", "on_time"])["RSSI"].agg(["mean"]).astype(int)
                riss_diff_df.reset_index(inplace=True)

                # srcmac_unique와 rssi mean 병합
                merged_srcmac_riss_df = pd.merge(srcmac_unique_df, riss_diff_df, left_on="SRCMAC", right_on="SRCMAC", how="left")
                merged_srcmac_riss_df = merged_srcmac_riss_df[merged_srcmac_riss_df["time_diff"] >= pd.Timedelta(cumsum)].reset_index(drop=True)
                merged_srcmac_riss_df.columns = ["SRCMAC", "time_min", "time_max", "time_diff", "on_time", "rssi_mean"]

                # on_time 별 People Count 계산
                merged_srcmac_riss_df = merged_srcmac_riss_df.groupby("on_time")[["SRCMAC"]].nunique().reset_index()

                if is_save == True:
                    # save 폴더가 없으면 save 폴더 생성
                    if not os.path.exists(os.path.join(self.args.path, "save")):
                        os.makedirs(os.path.join(self.args.path, "save"))
                        logger.info(f"The save folder has been created!")
                    
                    # location 별 SRCMAC 목록 데이터 저장
                    save_file = f"{self.args.save_data[:5]}_일자별 시간대별 체류인원수.csv"
                    merged_srcmac_riss_df.to_csv(f"{os.path.join(self.args.path, "save", save_file)}", index=False, encoding="utf-8-sig")
                    logger.info(f"{save_file} has been created!")
            
            elif on_time == False:
                # rssi mean 계산
                riss_diff_df = raw_data_df.groupby("SRCMAC")["RSSI"].agg(["mean"]).astype(int)
                riss_diff_df.reset_index(inplace=True)

                # srcmac_unique와 rssi mean 병합
                merged_srcmac_riss_df = pd.merge(srcmac_unique_df, riss_diff_df, left_on="SRCMAC", right_on="SRCMAC", how="left")
                merged_srcmac_riss_df = merged_srcmac_riss_df[merged_srcmac_riss_df["time_diff"] >= pd.Timedelta(cumsum)].reset_index(drop=True)
                merged_srcmac_riss_df.columns = ["SRCMAC", "time_min", "time_max", "time_diff", "rssi_mean"]

                if is_save == True:
                    # save 폴더가 없으면 save 폴더 생성
                    if not os.path.exists(os.path.join(self.args.path, "save")):
                        os.makedirs(os.path.join(self.args.path, "save"))
                        logger.info(f"The save folder has been created!")
                    
                    # location 별 SRCMAC 목록 데이터 저장
                    save_file = f"{self.args.save_data[:5]}_일자별 체류인원항목.csv"
                    merged_srcmac_riss_df.to_csv(f"{os.path.join(self.args.path, "save", save_file)}", index=False, encoding="utf-8-sig")
                    logger.info(f"{save_file} has been created!")

            logger.info(f"People Count by Non-Location : {merged_srcmac_riss_df.shape}")

            return merged_srcmac_riss_df, raw_data_df

    def calc_loc_people_spenttime(self, raw_data, srcmac_unique_df, base_time=6, time_diff="00:01:00", loc=False, entry_time=False, is_save=False):
        # 1시간 단위로 그룹화하기 위해 hour 컬럼 추가
        raw_data["hour"] = raw_data["TIME_KST"].dt.floor("h")
        
        # 시계열 순으로 정렬
        raw_data = raw_data.sort_values(by="TIME_KST", ascending=True).reset_index(drop=True)

        # TIME_KST hh:mm:ss 포멧 추가
        raw_data["TIME_KST(hh:mm:ss)"] = pd.to_datetime(raw_data["TIME_KST"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        raw_data["TIME_KST(hh:mm:ss)"] = pd.to_datetime(raw_data["TIME_KST(hh:mm:ss)"])

        # 동일 시간에 SRCMAC이 중복 시 max rssi만 추출
        raw_data = raw_data.loc[raw_data.groupby(["TIME_KST(hh:mm:ss)", "SRCMAC"])["RSSI"].idxmax()]

        # 시계열 순으로 정렬
        raw_data = raw_data.sort_values(by="TIME_KST", ascending=True).reset_index(drop=True)

        # srcmac unique만 추출
        _srcmac_unique_df = raw_data[raw_data["SRCMAC"].isin(srcmac_unique_df["SRCMAC"])].reset_index(drop=True)

        if loc == True:
            """
            공통적으로 location별, hour 정보가 필요
            hour 정보가 없으면 location별 입장, 퇴장 시간으로 계산됨
            """
            # 각 hour, SRCMAC, locatioon 별 min time과 max time을 계산
            h_src_loc_df = _srcmac_unique_df.groupby(["SRCMAC", "location", "hour"])["TIME_KST(hh:mm:ss)"].agg(["min", "max"])
            h_src_loc_df["time_diff"] = h_src_loc_df["max"] - h_src_loc_df["min"]

            # 각 hour, SRCMAC, locatioon 별 rssi mean을 계산
            rssi_df = _srcmac_unique_df.groupby(["SRCMAC", "location", "hour"])["RSSI"].agg(["mean"]).round(0)

            # h_src_loc_df & rssi_df merge
            both_df = pd.merge(h_src_loc_df, rssi_df, left_index=True, right_index=True).reset_index()

            # SRCMAC 별 time_diff의 평균이 10시간 이상인 SRCMAC 제거
            time_diff_sum = both_df.groupby("SRCMAC")["time_diff"].sum()
            over_basetime_index = time_diff_sum[time_diff_sum < pd.Timedelta(hours=10)].index
            over_basetime_both_df = both_df[both_df["SRCMAC"].isin(over_basetime_index)].reset_index(drop=True)

            # time_diff 시간 제거
            over_basetime_both_df = over_basetime_both_df[over_basetime_both_df["time_diff"] > pd.Timedelta("00:01:00")].reset_index(drop=True)

            """entry time을 포함"""
            if entry_time == True:
                # hour 컬럼을 이용하여 entry_time을 계산
                hour_min_df = over_basetime_both_df.groupby(["SRCMAC", "location"])[["hour"]].agg("min")
                hour_min_df.columns = ["hour_min"]

                # hour_min_df와 over_basetime_both_df merge를 위한 멀티인덱스 설정 및 merge
                over_basetime_both_df = over_basetime_both_df.set_index(["SRCMAC", "location"])
                hour_min_both_df = pd.merge(over_basetime_both_df, hour_min_df, left_index=True, right_index=True).reset_index()

                # SRCMAC, location, hour_max별 time_diff의 합
                src_loc_h_spenttime_mean_df = hour_min_both_df.groupby(["SRCMAC", "location", "hour_min"])[["time_diff"]].agg("sum").reset_index()

                # location별 평균 time_diff
                loc_h_spenttime_mean_df = src_loc_h_spenttime_mean_df.groupby(["location", "hour_min"])[["time_diff"]].agg("mean").reset_index()

                if is_save == True:
                    # save 폴더가 없으면 save 폴더 생성
                    if not os.path.exists(os.path.join(self.args.path, "save")):
                        os.makedirs(os.path.join(self.args.path, "save"))
                        logger.info(f"The save folder has been created!")
                    
                    # location 별 SRCMAC 목록 데이터 저장
                    save_file = f"{self.args.save_data[:5]}_일자별 공간별 시간대별 체류시간.csv"
                    loc_h_spenttime_mean_df.to_csv(f"{os.path.join(self.args.path, "save", save_file)}", index=False, encoding="utf-8-sig")
                    logger.info(f"{save_file} has been created!")

                return loc_h_spenttime_mean_df
            
            """entry time을 미포함"""
             # SRCMAC, location별 time_diff의 합
            src_loc_spenttime_mean_df = over_basetime_both_df.groupby(["SRCMAC", "location"])[["time_diff"]].agg("sum").reset_index()

            # location별 time_diff의 평균
            loc_spenttime_mean_df = src_loc_spenttime_mean_df.groupby("location")[["time_diff"]].agg("mean").reset_index()

            if is_save == True:
                # save 폴더가 없으면 save 폴더 생성
                if not os.path.exists(os.path.join(self.args.path, "save")):
                    os.makedirs(os.path.join(self.args.path, "save"))
                    logger.info(f"The save folder has been created!")
                
                # location 별 SRCMAC 목록 데이터 저장
                save_file = f"{self.args.save_data[:5]}_일자별 공간별 체류시간.csv"
                loc_h_spenttime_mean_df.to_csv(f"{os.path.join(self.args.path, "save", save_file)}", index=False, encoding="utf-8-sig")
                logger.info(f"{save_file} has been created!")

            return loc_spenttime_mean_df, over_basetime_both_df

        elif loc == False:
            pass

    def create_srcmac_loc_sequence_metrix(self, df):
        _df = df.groupby(["SRCMAC", "location"])[["time_diff"]].mean().reset_index()
        
        # 각 SRCMAC 별로 시간순으로 location 방문 순서 부여
        _df["loc_sequence"] = _df.groupby("SRCMAC").cumcount() + 1

        # location을 열로 변환하고 loc_sequence 값을 채우는 pivot table
        srcmac_loc_seq_metrix = _df.pivot_table(index="SRCMAC", columns="location", values="loc_sequence", fill_value=0)
        srcmac_loc_seq_metrix.reset_index(inplace=True)
        srcmac_loc_seq_metrix.columns.name = None # index의 이름 제거
        srcmac_loc_seq_metrix = srcmac_loc_seq_metrix.astype(int) # float를 int로 변환

        logger.info(f"SRCMAC Location Sequence Metrix Shape : {srcmac_loc_seq_metrix.shape}")

        return srcmac_loc_seq_metrix

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(name)s -   %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        level=logging.INFO
    )

    cli_parser = argparse.ArgumentParser()
    cli_args = cli_parser.parse_args("")

    cli_args.config_dir = "config"
    cli_args.config_file = "config.json"
    
    with open(os.path.join(cli_args.config_dir, cli_args.config_file)) as f:
        args = AttrDict(json.load(f))

    prep = Preprocessing(args)
    pre_raw_data = prep.get_preprocessed_data(raw_data="gangneung_rawData_20241025.csv", time=True)
    srcmac_unique_df = prep.calc_srcmac_unique(pre_raw_data)
    loc_people_cnt = prep.calc_loc_people_counts(pre_raw_data, srcmac_unique_df, loc=True)
    h_loc_spenttime_mean_df, over_basetime_both_df = prep.calc_loc_people_spenttime(pre_raw_data, srcmac_unique_df, loc=True)
    srcmac_loc_seq_metrix = prep.create_srcmac_loc_sequence_metrix(over_basetime_both_df)