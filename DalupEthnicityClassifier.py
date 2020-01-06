import pandas as pd
from DLModels import RNN
from EthnicityClassifier import EthnicityClassifier
from tqdm import tqdm

def get_ethnicity_info(first_name, last_name):
    classifier = EthnicityClassifier()
    if first_name is None:
        first_name = ""
    if last_name is None:
        last_name = ""
    full_name = str(first_name) + " " + str(last_name)
    print(full_name)
    predicted_ethnicity_full, probablities_full = classifier.predict(full_name.upper())
    print('full-->'+str(probablities_full))
    if first_name == "":
        predicted_ethnicity_first = -1
        probablities_first = -1
    else:
        predicted_ethnicity_first, probablities_first = classifier.predict(str(first_name).upper())
    if last_name == "":
        predicted_ethnicity_last = -1
        probablities_last = -1
    else:
        predicted_ethnicity_last, probablities_last = classifier.predict(str(last_name).upper())

    print(predicted_ethnicity_full)
    return predicted_ethnicity_full, predicted_ethnicity_first, predicted_ethnicity_last, probablities_full, probablities_first, probablities_last


#customer_names = pd.read_csv("Dalup_Unique_customer_names.csv")
#customer_names_with_ethnicity = pd.DataFrame(columns=("Last Name", "First Name", "Probability Indian Full Name", "Probability Indian First Name", "Probability Indian Last Name"))

#for i in tqdm(range(len(customer_names))):
 #   customer = customer_names.iloc[i]
  #  _, _, _, probablities, probablities_first, probablities_last = get_ethnicity_info(first_name = customer["Last Name"], last_name = customer["First Name"])
   # customer_names_with_ethnicity = customer_names_with_ethnicity.append({"Last Name": str(customer["Last Name"]), "First Name": str(customer["First Name"]), "Probability Indian Full Name": probablities[1], "Probability Indian First Name": probablities_first[1], "Probability Indian Last Name": probablities_last[1]}, ignore_index=True)

#customer_names_with_ethnicity.to_csv("Dalup_Unique_customer_names_ethnicity.csv")