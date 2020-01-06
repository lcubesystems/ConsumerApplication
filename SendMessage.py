import pickle

import requests
from os import path
import datetime

try:
    date_of_birth = datetime.datetime.strptime('7/23/1932', "%d/%m/%Y")
    print('correct Date -- > ')
except:
    print('Incorrect Date -- > ' )
if path.exists('models/fax_classification_model.mod'):
    print(datetime.datetime.now())
    document_category_prediction_model = pickle.load(open("models/fax_classification_model.mod", "rb"))
    print(datetime.datetime.now())
    document_category_prediction_model = pickle.load(open("models/fax_classification_model.mod", "rb"))
    print(datetime.datetime.now())
    print(document_category_prediction_model)
else:
    print('not ecist'+str(path.exists('models/fax_classification_model_v1.mod')))
url = "https://www.fast2sms.com/dev/bulk"

payload = "sender_id=FSTSMS&message=test&language=english&route=p&numbers=9999999999,888888888"
headers = {
    'authorization': "YOUR_AUTH_KEY",
    'Content-Type': "application/x-www-form-urlencoded",
    'Cache-Control': "no-cache",
}

response = requests.request("POST", url, data=payload, headers=headers)

print(response.text)