import os
import logging
import pandas as pd
# from tqdm import tqdm

def init_logger():
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(name)s -   %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        level=logging.INFO,
    )

class MergeFiles:
    """
    추후 수정
    """
    def list_files_in_directory(self, dir_path):
        try:
            files = os.listdir(dir_path)
            return files
        
        except FileNotFoundError:
            return f"The directory {dir_path} does not exist."
        
        except PermissionError:
            return f"Permission denied for accessing {dir_path}."
        
        except Exception as e:
            return str(e)
    
    def merge_files(self, dir_path, save_file, encoding="utf-8"):
        dfs = []
        _files = self.list_files_in_directory(dir_path)

        for file in _files:
            _, file_extension = os.path.splitext(os.path.join(dir_path, file))
        
            if file_extension == ".csv":
                df = pd.read_csv(os.path.join(dir_path, file), encoding=encoding)
            
            elif file_extension in [".xls", ".xlsx"]:
                df = pd.read_excel(os.path.join(dir_path, file))
            
            else:
                raise ValueError("Only CSV or Excel files are accepted.")

            print(f"{file} DataFrame Shape : {df.shape}")
            dfs.append(df)

        merged_df = pd.concat(dfs).reset_index(drop=True)
        print(f"Merged DataFrame Shape : {merged_df.shape}")
        merged_df.to_csv(os.path.join(dir_path, f"{save_file}.csv"), encoding="utf-8-sig")

if __name__ == "__main__":
    mf_obj = MergeFiles()
    # files = mf_obj.list_files_in_directory("./data/kme2024/save/merge")
    mf_obj.merge_files("./data/gangneung/save/merge", "통합_강릉커피축제 일자별 공간별 시간별 체류인원항목")