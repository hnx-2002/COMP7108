from pymongo import MongoClient
import pandas as pd
import folium
from folium.plugins import HeatMap
import branca.colormap as cm

# Connect to local MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client["airquality"]
collection = db["nairobi_data"]

# Build the latitude/longitude dictionary for the location
location_coords = {}
for document in collection.find({}):
    location = document.get('location')
    lat = document.get('lat')  # Get the latitude value
    lon = document.get('lon')  # Get the longitude value
    if location is not None and lat is not None and lon is not None:
        # Store location as a key and latitude/longitude as a value
        location_coords[location] = {'lat': lat, 'lon': lon}


def calculate_air_quality_score(p1, p2, p0, temperature, humidity):
    weight_p1 = 0.30
    weight_p2 = 0.25
    weight_p0 = 0.20
    weight_temperature = 0.15
    weight_humidity = 0.10
    # Maximum reasonable value
    max_p1 = 150
    max_p2 = 75
    max_p0 = 50
    optimal_temp = 20
    optimal_humidity = 50

    score_p1 = max(0, 1 - (p1 / max_p1)) if p1 <= max_p1 else 0
    score_p2 = max(0, 1 - (p2 / max_p2)) if p2 <= max_p2 else 0
    score_p0 = max(0, 1 - (p0 / max_p0)) if p0 <= max_p0 else 0
    score_temperature = max(0, 1 - abs(temperature - optimal_temp) / 10)
    score_humidity = max(0, 1 - abs(humidity - optimal_humidity) / 20)

    air_quality_score = (
                                weight_p1 * score_p1 +
                                weight_p2 * score_p2 +
                                weight_p0 * score_p0 +
                                weight_temperature * score_temperature +
                                weight_humidity * score_humidity
                        ) * 100

    return round(air_quality_score, 2)


# 读取CSV文件
df = pd.read_csv("../Data/monthly_avg_by_location.csv")

# 过滤出四大指标的数据
filtered_df = df[df['value_type'].isin(['P1', 'P2', 'P0', 'temperature', 'humidity'])]

# Calculate the average of the four main indicators for each location
location_data = {}
for location in filtered_df['location'].unique():
    location_df = filtered_df[filtered_df['location'] == location]

    avg_p1 = location_df[location_df['value_type'] == 'P1']['avg_value'].mean()
    avg_p2 = location_df[location_df['value_type'] == 'P2']['avg_value'].mean()
    avg_p0 = location_df[location_df['value_type'] == 'P0']['avg_value'].mean()
    avg_temperature = location_df[location_df['value_type'] == 'temperature']['avg_value'].mean()
    avg_humidity = location_df[location_df['value_type'] == 'humidity']['avg_value'].mean()

    location_data[location] = {
        'P1': avg_p1,
        'P2': avg_p2,
        'P0': avg_p0,
        'temperature': avg_temperature,
        'humidity': avg_humidity
    }

# 计算每个location的空气质量分数
location_scores = {}
for location, data in location_data.items():
    score = calculate_air_quality_score(
        data['P1'],
        data['P2'],
        data['P0'],
        data['temperature'],
        data['humidity']
    )
    location_scores[location] = score

# Create heat maps
nairobi_map = folium.Map(location=[-1.2921, 36.8219], zoom_start=12)
# Add a heat map layer
heat_data = []
for location, score in location_scores.items():
    if location in location_coords:
        coords = location_coords[location]
        # Converts to a range of 0-100, with higher scores being lighter in color
        heat_data.append([coords['lat'], coords['lon'], 100 - score])
HeatMap(heat_data).add_to(nairobi_map)

# 添加箭头标记
for location, coords in location_coords.items():
    if location in location_scores:
        folium.Marker(
            location=[coords['lat'], coords['lon']],
            icon=folium.Icon(color='blue', icon=''),
            popup=f"Location {location}: Score {location_scores[location]}"
        ).add_to(nairobi_map)

# 添加颜色和分数对应的注释
colormap = cm.LinearColormap(colors=['red','orange','yellow','green'],
                             index=[0, 25, 50, 75],
                             vmin=0,
                             vmax=100,
                             caption='Air Quality Score')
nairobi_map.add_child(colormap)

# 将热力图保存为HTML文件
nairobi_map.save("air_quality_heatmap.html")