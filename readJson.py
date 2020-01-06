import json
import pickle
import datetime
import pandas as pd
import os

labels = pd.read_csv("labels.csv")

with open('../../data.json') as json_file:
    data = json.load(json_file)

test_data = []

for entry in data[0][0]:
    predicted_class = entry["predictedclass"]
    is_cover_page = entry["iscover"]
    full_content = list()
    full_content = entry["content"]
    corrected_class = entry["correctedclass"]

    if corrected_class == "Other":
        continue

    doc_object = {}
    doc_object["is_cover_page"] = is_cover_page
    doc_object["pages"] = full_content
    doc_object["class_name"] = corrected_class
    doc_object["label"] = labels.loc[labels["name"] == corrected_class]["index"].values[0]
    doc_object["page_count"] = len(full_content)

    print(predicted_class+" - "+corrected_class)

    test_data.append(doc_object)

print(len(test_data))

#pickle.dump(test_data, open("../../Image-Text/data/data_"+str(datetime.date.today())+".p", "wb"))
