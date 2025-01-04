import pandas as pd
import json
import os
import datetime
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
import shutil
from pymongo import MongoClient
from bson import ObjectId

def download_dataset():
    # Authenticate to Kaggle
    api = KaggleApi()
    api.authenticate()

    # Create dirs if missing
    data_dir = './data'
    archived_data_dir = './archived_data'
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(archived_data_dir, exist_ok=True)

    # Downloads dataset
    try:
        api.dataset_download_files("chitrakumari25/smart-agricultural-production-optimizing-engine", path=data_dir)
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        return

    # Unzips dataset
    zip_file_path = os.path.join(data_dir, 'smart-agricultural-production-optimizing-engine.zip')
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir)
    except Exception as e:
        print(f"Error extracting zip file: {e}")
        return

    original_filename = 'Crop_recommendation.csv'
    new_filename = 'IoT_Crop_Soils.csv'

    # File paths
    original_path = os.path.join(data_dir, original_filename)
    new_path = os.path.join(data_dir, new_filename)

    # if theres existing json, archive and replace
    json_file_path = os.path.join(data_dir, 'soil_data.json')
    if os.path.exists(json_file_path):
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        archived_json_filename = f"soil_data_{timestamp}.json"
        shutil.move(json_file_path, os.path.join(archived_data_dir, archived_json_filename))

    # Rename extracted file
    os.rename(original_path, new_path)

    # Load dataset
    try:
        soil_data = pd.read_csv(new_path)
        # Convert csv to JSON
        json_data = soil_data.to_dict(orient='records')
        # Function to convert ObjectId to string
        def convert_objectid_to_str(data):
            for item in data:
                for key, value in item.items():
                    if isinstance(value, ObjectId):
                        item[key] = str(value)
            return data
        # Convert any ObjectId to string
        json_data = convert_objectid_to_str(json_data)
        
        # MongoDB connection
        client = MongoClient("mongodb://localhost:27017/") 
        db = client["agriculture_db"]  
        collection = db["crop_data"]  

        # Insert JSON to MongoDB
        collection.insert_many(json_data)
        print("Data successfully inserted into MongoDB.")

        # Custom JSON serializer to handle ObjectId
        def custom_json_serializer(obj):
            if isinstance(obj, ObjectId):
                return str(obj)
            raise TypeError(f"Type {type(obj)} not serializable")

        # JSON write serializer
        with open(json_file_path, 'w') as f:
            json.dump(json_data, f, default=custom_json_serializer, indent=4)

        # Delete CSV after JSON is made
        os.remove(new_path)
        print(f"Deleted the CSV file: {new_filename}")
    except Exception as e:
        print(f"Error processing dataset: {e}")
        return

if __name__ == "__main__":
    download_dataset()
