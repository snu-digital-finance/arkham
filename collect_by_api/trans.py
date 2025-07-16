import pandas as pd
import numpy as np
import os
import glob

# 모든 .parquet 파일 경로를 찾기
directory = '../../data/ethereum_token_transfer'
file_paths = glob.glob(os.path.join(directory, '**', '*.parquet'), recursive=True)
print(file_paths)
# 각 파일을 읽어 DataFrame으로 변환하고 리스트에 저장
dataframes = pd.read_parquet(file_paths[0])
print(f"📂 {file_paths[0]} 파일에서 데이터 읽기 완료. 행 수: {len(dataframes)}")
print(dataframes.columns)
# addr = np.concatenate([dataframes['From'].unique(), dataframes['To'].unique()])

for file_path in file_paths[1:]:
    df = pd.read_parquet(file_path)
    dataframes = pd.concat([dataframes, df], ignore_index=True, ).drop_duplicates()
    break

# 모든 DataFrame을 하나로 합치기
combined_df = pd.concat(dataframes, ignore_index=True)