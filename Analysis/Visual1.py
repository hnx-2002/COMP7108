import pandas as pd
import plotly.express as px
from pathlib import Path
import os

# 读取 CSV 文件
csv_path = Path("./Data/monthly_avg_by_location.csv")
df = pd.read_csv(csv_path)

# 构造时间列（横轴）
df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))

# 所有地点
locations = df["location"].unique()

# 为每个地点生成一张交互式折线图（自动跳过缺失月份）
for loc in locations:
    subset = df[df["location"] == loc]
    
    fig = px.line(
        subset,
        x="date",
        y="avg_value",
        color="value_type",
        title=f"Location {loc} - Monthly Average Sensor Readings",
        markers=True,
        labels={
            "date": "Time",
            "avg_value": "Average Value",
            "value_type": "Sensor Type"
        }
    )

    # 保存为 HTML 文件
    fig.write_html(f"./Results/plot_location_{loc}.html")

print("✅ 图表生成完成，每个地点的 HTML 文件保存在 ./Data/ 目录中")
