import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

def visualise_data():
    client = MongoClient("mongodb://localhost:27017/")  
    db = client["agriculture_db"]  
    collection = db["crop_data"]  

    crop_labels = collection.distinct('label')
    features = ['N', 'P', 'K', 'temperature', 'humidity', 'ph', 'rainfall']

    avg_data = pd.DataFrame(index=crop_labels, columns=features)

    for crop in crop_labels:
        crop_data = list(collection.find({'label': crop}))
        df = pd.DataFrame(crop_data)
        avg_data.loc[crop] = df[features].mean().values

    for col in avg_data.columns:
        avg_data[col] = pd.to_numeric(avg_data[col])

    avg_data = avg_data.round(2) 

    fig, ax = plt.subplots(figsize=(24, 12))  
    ax.axis('off')

    table = ax.table(cellText=avg_data.values, 
                     rowLabels=avg_data.index, 
                     colLabels=avg_data.columns,
                     cellLoc='center', 
                     loc='center')

    for (row, col), cell in table.get_celld().items():
        if (row == 0) or (col == -1): 
            cell.set_facecolor('lightblue')

    for (row, col), cell in table.get_celld().items():
        if (row > 0) and (col >= 0):
            value = avg_data.iloc[row-1, col]

            if col == features.index('temperature'):
                if value > 28:
                    cell.set_facecolor('red')
                elif value > 25:
                    cell.set_facecolor('orange')
                else:
                    cell.set_facecolor('lightgreen')
            else:
                cell.set_facecolor('white')

    table.auto_set_font_size(False)
    table.set_fontsize(8)  
    table.scale(2, 2)  

    plt.title("Average Crop Feature Values")
    plt.subplots_adjust(left=0.333, bottom=0.062, right=0.724, top=0.893, wspace=0.2, hspace=0.193)

    plt.show()

if __name__ == "__main__":
    visualise_data()