import pandas as pd
import plotly.express as px
from pathlib import Path
import os

csv_path = Path("./Data/monthly_avg_by_location.csv")
df = pd.read_csv(csv_path)

df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))

locations = df["location"].unique()

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

    fig.write_html(f"./Results/plot_location_{loc}.html")

print("The chart generation is complete!")
