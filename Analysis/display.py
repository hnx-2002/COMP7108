import os

import pandas as pd
from pymongo import MongoClient
import folium

client = MongoClient('localhost', 27017)
db = client['nairobi']
collection = db['data']

documents = collection.find({},
                            {'_id': 0, 'lat': 1, 'lon': 1, 'location': 1, 'timestamp': 1, 'value': 1, 'value_type': 1})

lat_lon_combinations = {}
for doc in documents:
    lat_lon = (doc['lat'], doc['lon'], doc['location'])
    if lat_lon not in lat_lon_combinations:
        lat_lon_combinations[lat_lon] = []
    lat_lon_combinations[lat_lon].append(doc)

if lat_lon_combinations:
    lats, lons, sensor_ids = zip(*lat_lon_combinations.keys())
    center_lat = sum(lats) / len(lats)
    center_lon = sum(lons) / len(lons)
    map_center = [center_lat, center_lon]
    zoom_level = 13
else:
    map_center = [0, 0]
    zoom_level = 2

m = folium.Map(location=map_center, zoom_start=zoom_level)

for lat_lon in lat_lon_combinations.keys():
    lat, lon, location = lat_lon

    recent_docs_temperature = list(collection.find(
        {'lat': lat, 'lon': lon, 'value_type': "temperature"},
        {'_id': 0, 'location': 1, 'timestamp': 1, 'value': 1, 'value_type': 1}
    ).sort('timestamp', -1).limit(5))

    while len(recent_docs_temperature) < 5:
        recent_docs_temperature.append({
            'location': location,
            'timestamp': "null",
            'value': "null",
            'value_type': "null"
        })

    recent_docs_humidity = list(collection.find(
        {'lat': lat, 'lon': lon, 'value_type': "humidity"},
        {'_id': 0, 'location': 1, 'timestamp': 1, 'value': 1, 'value_type': 1}
    ).sort('timestamp', -1).limit(5))

    while len(recent_docs_humidity) < 5:
        recent_docs_humidity.append({
            'location': location,
            'timestamp': "null",
            'value': "null",
            'value_type': "null"
        })

    recent_docs_P0 = list(collection.find(
        {'lat': lat, 'lon': lon, 'value_type': "P0"},
        {'_id': 0, 'location': 1, 'timestamp': 1, 'value': 1, 'value_type': 1}
    ).sort('timestamp', -1).limit(5))

    while len(recent_docs_P0) < 5:
        recent_docs_P0.append({
            'location': location,
            'timestamp': "null",
            'value': "null",
            'value_type': "null"
        })

    recent_docs_P1 = list(collection.find(
        {'lat': lat, 'lon': lon, 'value_type': "P1"},
        {'_id': 0, 'location': 1, 'timestamp': 1, 'value': 1, 'value_type': 1}
    ).sort('timestamp', -1).limit(5))

    while len(recent_docs_P1) < 5:
        recent_docs_P1.append({
            'location': location,
            'timestamp': "null",
            'value': "null",
            'value_type': "null"
        })

    recent_docs_P2 = list(collection.find(
        {'lat': lat, 'lon': lon, 'value_type': "P2"},
        {'_id': 0, 'location': 1, 'timestamp': 1, 'value': 1, 'value_type': 1}
    ).sort('timestamp', -1).limit(5))

    while len(recent_docs_P2) < 5:
        recent_docs_P2.append({
            'location': location,
            'timestamp': "null",
            'value': "null",
            'value_type': "null"
        })
    if not os.path.exists("data"):
        os.makedirs("data", exist_ok=True)

    df_temperature = pd.DataFrame(recent_docs_temperature)
    df_temperature.to_csv(f"data/recent_docs_temperature_{location}.csv", index=False)

    df_humidity = pd.DataFrame(recent_docs_humidity)
    df_humidity.to_csv(f"data/recent_docs_humidity_{location}.csv", index=False)

    df_P0 = pd.DataFrame(recent_docs_P0)
    df_P0.to_csv(f"data/recent_docs_P0_{location}.csv", index=False)

    df_P1 = pd.DataFrame(recent_docs_P1)
    df_P1.to_csv(f"data/recent_docs_P1_{location}.csv", index=False)

    df_P2 = pd.DataFrame(recent_docs_P2)
    df_P2.to_csv(f"data/recent_docs_P2_{location}.csv", index=False)

    # popup_html = "<table><tr><th>Sensor ID</th><th>Timestamp</th><th>Value Type</th><th>Value</th></tr>"
    # for doc in recent_docs:
    #     popup_html += f"<tr><td>{doc['sensor_id']}</td><td>{doc['timestamp']}</td><td>{doc['value_type']}</td><td>{doc['value']}</td></tr>"
    # popup_html += "</table>"

    # popup_content = (f"Location: {location}\n"
    #                  f"Recent temperature: from {recent_docs_temperature[4]['timestamp']} to {recent_docs_temperature[0]['timestamp']}\n"
    #                  f"values: {recent_docs_temperature[0]['value'], recent_docs_temperature[1]['value'], recent_docs_temperature[2]['value'], recent_docs_temperature[3]['value'], recent_docs_temperature[4]['value']}\n"
    #                  f"Recent humidity: from {recent_docs_humidity[4]['timestamp']} to {recent_docs_humidity[0]['timestamp']}\n"
    #                  f"values: {recent_docs_humidity[0]['value'], recent_docs_humidity[1]['value'], recent_docs_humidity[2]['value'], recent_docs_humidity[3]['value'], recent_docs_humidity[4]['value']}\n"
    #                  f"Recent P0: from {recent_docs_P0[4]['timestamp']} to {recent_docs_P0[0]['timestamp']}\n"
    #                  f"values: {recent_docs_P0[0]['value'], recent_docs_P0[1]['value'], recent_docs_P0[2]['value'], recent_docs_P0[3]['value'], recent_docs_P0[4]['value']}\n"
    #                  f"Recent P1: from {recent_docs_P1[4]['timestamp']} to {recent_docs_P1[0]['timestamp']}\n"
    #                  f"values: {recent_docs_P1[0]['value'], recent_docs_P1[1]['value'], recent_docs_P1[2]['value'], recent_docs_P1[3]['value'], recent_docs_P1[4]['value']}\n"
    #                  f"Recent P2: from {recent_docs_P2[4]['timestamp']} to {recent_docs_P2[0]['timestamp']}\n"
    #                  f"values: {recent_docs_P2[0]['value'], recent_docs_P2[1]['value'], recent_docs_P2[2]['value'], recent_docs_P2[3]['value'], recent_docs_P2[4]['value']}\n"
    #                  )
    # folium.Marker([lat, lon], popup=popup_content).add_to(m)

    popup_content = (f"<b>Location:</b> {location}<br>"
                     f"<b>Recent temperature:</b> from {recent_docs_temperature[4]['timestamp']} to {recent_docs_temperature[0]['timestamp']}<br>"
                     f"<b>values:</b> {', '.join(str(doc['value']) for doc in recent_docs_temperature)}<br>"
                     f"<b>Recent humidity:</b> from {recent_docs_humidity[4]['timestamp']} to {recent_docs_humidity[0]['timestamp']}<br>"
                     f"<b>values:</b> {', '.join(str(doc['value']) for doc in recent_docs_humidity)}<br>"
                     f"<b>Recent P0:</b> from {recent_docs_P0[4]['timestamp']} to {recent_docs_P0[0]['timestamp']}<br>"
                     f"<b>values:</b> {', '.join(str(doc['value']) for doc in recent_docs_P0)}<br>"
                     f"<b>Recent P1:</b> from {recent_docs_P1[4]['timestamp']} to {recent_docs_P1[0]['timestamp']}<br>"
                     f"<b>values:</b> {', '.join(str(doc['value']) for doc in recent_docs_P1)}<br>"
                     f"<b>Recent P2:</b> from {recent_docs_P2[4]['timestamp']} to {recent_docs_P2[0]['timestamp']}<br>"
                     f"<b>values:</b> {', '.join(str(doc['value']) for doc in recent_docs_P2)}<br>"
                     )

    folium.Marker([lat, lon], popup=folium.Popup(popup_content, max_width=300)).add_to(m)

    popup = folium.Popup(popup_content, max_width=300)

    folium.Marker([lat, lon], popup=popup).add_to(m)

m.save("map.html")
