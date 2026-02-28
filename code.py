id,name,salary
1,Alice,50000
2,Bob,60000
3,Charlie,55000
4,David,70000
import pandas as pd

# Extract
df = pd.read_csv("input_data.csv")

# Transform
df["salary_after_tax"] = df["salary"] * 0.9

# Load
df.to_csv("output_data.csv", index=False)

print("ETL completed successfully!")