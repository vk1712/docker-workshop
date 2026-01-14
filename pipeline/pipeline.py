import sys
import pandas as pd

month = sys.argv[1]

df = pd.DataFrame({"day": [1, 2],
                   "num_passengers": [3, 4]})
df['month'] = month

df.to_parquet(f"output_file.parquet")

print(f"hey!")
print(df.head())