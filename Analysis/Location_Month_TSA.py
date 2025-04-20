from pymongo import MongoClient
import pandas as pd
import os

save_dir = "/Data"
save_path = os.path.join(save_dir, "monthly_avg_by_location.csv")

print(save_path)

os.makedirs(save_dir, exist_ok=True)

# MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["airquality"]
collection = db["nairobi_data"]

# aggregate
pipeline = [
    {
        "$match": {
            "value_type": {"$in": ["P0", "P1", "P2", "temperature", "humidity"]}
        }
    },
    {
        "$project": {
            "location": 1,
            "value_type": 1,
            "value": 1,
            "year": {"$year": {"$toDate": "$timestamp"}},
            "month": {"$month": {"$toDate": "$timestamp"}}
        }
    },
    {
        "$group": {
            "_id": {
                "location": "$location",
                "year": "$year",
                "month": "$month",
                "value_type": "$value_type"
            },
            "avg_value": {"$avg": "$value"}
        }
    },
    {
        "$sort": {
            "_id.location": 1,
            "_id.year": 1,
            "_id.month": 1
        }
    }
]

# query
results = list(collection.aggregate(pipeline))

# to dataframe
data = []
for item in results:
    data.append({
        "location": item["_id"]["location"],
        "year": item["_id"]["year"],
        "month": item["_id"]["month"],
        "value_type": item["_id"]["value_type"],
        "avg_value": round(item["avg_value"], 2)
    })

df = pd.DataFrame(data)

# to csv
df.to_csv(save_path, index=False)
print(f"Save to: {save_path}")