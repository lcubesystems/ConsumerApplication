from builtins import print

import flask
from flask import Flask
from flask import request, jsonify, render_template, flash, redirect, json

from EthnicityClassifier import EthnicityClassifier
from DLModels import RNN
app = Flask(__name__)
import logging
import requests
import KafkaLogs, Logging
import torch
import string
from tqdm import tqdm


Logging.configure_logging('/tmp/daluplog')
logging.warning('Gender API Started successfully')

from Gender import GenderClassifier
@app.route('/predict_weather', methods=['GET', 'POST'])
def getWeatherInfo():
    if request.method == 'GET':
        if 'zip' in request.args:
            zip = str(request.args['zip'])
            try:
                logging.info('zip--> : '+str(zip))

                url = "https://api.openweathermap.org/data/2.5/weather?zip={}&appid={}".format(zip, '8b234e5319fe5076baae32946819193f')
                r = requests.get(url)
                try:
                    kfdb = KafkaLogs.connect()
                    cur = kfdb.cursor()
                    cur.execute('insert into hourly_weather (hourly_weather_response) values ($$' + json.dumps(
                        r.json()) + '$$)');
                except (Exception) as error:
                    print(error)
                    logging.warn(error)
                kfdb.commit()
                kfdb.close()
                return json.dumps(r.json())
            except (Exception) as error:
                result={"Error": "404"}
                return json.dumps(result)
@app.route('/predict_forecast', methods=['GET', 'POST'])
def getWeatherforecastInfo():
    if request.method == 'GET':
            try:

                #logging.info('zip--> : '+str(zip))

                url = "https://api.openweathermap.org/data/2.5/forecast?lat=40.7482132&lon=-73.9929658&appid=8b234e5319fe5076baae32946819193f"
                r = requests.get(url)
                print('test')
                try:
                    kfdb = KafkaLogs.connect()
                    cur = kfdb.cursor()
                    print(str(r.json()))
                    cur.execute('insert into forecast_weather (forecast_weather_response) values ($$' + json.dumps(
                        r.json()) + '$$)');
                except (Exception) as error:
                    print(error)
                    logging.warn(error)
                kfdb.commit()
                kfdb.close()

                return json.dumps(r.json())
            except (Exception) as error:
                result={"Error": "404"}
                return json.dumps(result)



@app.route('/predict_gender', methods=['GET', 'POST'])
def getEthni():
    print('predict_gender')
    if request.method == 'GET':
        if 'name' in request.args:
            name = str(request.args['name'])

        result={}
        try:
            logging.info('name--> : '+str(name))
            prediction=genderobj.predict(name)
            result['gender']=str(prediction)
            result['nationality']=''
        except (Exception) as error:
            result={"gender": "", "nationality": ""}
            return json.dumps(result)
            print(error)

    return str(json.dumps(result))

@app.route('/predict_gender_ethnicity', methods=['GET', 'POST'])
def getInfo():
    print('test')
    if request.method == 'GET':
        if 'fname' in request.args:
            first_name = str(request.args['fname']).strip()
            if str(first_name).strip()=='':
                first_name=None;
        if 'lname' in request.args:
            last_name = str(request.args['lname'])
            if last_name.isspace() and first_name==None:
                splited_name=last_name.split()
                first_name=splited_name[0];
                last_name=splited_name[1];
            print('last-->'+str(last_name))
            print('first-->' + str(first_name))
            if str(last_name).strip()=='':
                last_name=None;

        result={}
        try:
            logging.info('name--> : '+str(first_name) + " " + str(last_name))
            if(str(first_name)==None) :
                print('last name')
                prediction = genderobj.predict(str(last_name))
            elif '.' in str(first_name).lower() or str(first_name).lower()=='mr' or str(first_name).lower()=='ms' or str(first_name).lower()=='mrs' or str(first_name).lower()=='miss' or str(first_name).lower()=='mx' or str(first_name).lower()=='jr' or str(first_name).lower()=='sr' or 'personalised' in str(first_name).lower() or 'currency' in str(first_name).lower() or 'cardholder' in str(first_name).lower() or 'visa' in str(first_name).lower() or len(str(first_name))==1 :
                prediction = genderobj.predict(str(last_name))
            else:
                print('firstname')
                prediction = genderobj.predict(str(first_name))
            result['gender'] = str(prediction)
            logging.info(str(prediction))



            try:
                ethnicity= get_ethnicity_info(first_name,last_name)
                result['ethnicity'] = ethnicity
            except (Exception) as error:
                logging.info(error)
                result['ethnicity'] = ""


        except (Exception) as error:
            result={"gender": "", "ethnicity": ""}
            logging.info(error)
            return json.dumps(result)
            print(error)

        response = app.response_class(
            response=json.dumps(result),
            mimetype='application/json'
        )

    return response
@app.route('/NLPMonitor', methods=['GET', 'POST'])
def monitor():
   return '1|SUCCESS';


def get_ethnicity_info(first_name, last_name):
    booleanflag='false'
    prefixflag='false'
    zeroflag=0
    classifier = EthnicityClassifier()
    result = {}
    if first_name is None:
        first_name = ""
    if last_name is None:
        last_name = ""
    print('sdfasdf')
    if 'patel' in last_name.lower() or 'rao' in last_name.lower() or 'chopra' in last_name.lower() or 'singh' in last_name.lower() or 'parikh' in last_name.lower() or 'parekh' in last_name.lower() or 'reddy' in last_name.lower():
        booleanflag='true';
        print('Same name ')
    if '.' in first_name.lower() or first_name.lower()=='mr' or first_name.lower()=='ms' or first_name.lower()=='mrs' or first_name.lower()=='miss' or first_name.lower()=='mx' or first_name.lower()=='jr' or first_name.lower()=='sr' or len(first_name)==1 or 'personalised' in first_name.lower() or 'currency' in first_name.lower() or 'cardholder' in first_name.lower() or 'visa' in first_name.lower():
        prefixflag = 'true';
    if 'personalised' in last_name.lower() or 'currency' in last_name.lower() or 'cardholder' in last_name.lower() or 'visa' in last_name.lower():
        zeroflag=1;
    if 'false' in prefixflag and zeroflag==0:
        full_name = str(first_name) + " " + str(last_name)
    elif zeroflag==1:
        full_name = str(first_name)
    else:
        full_name = str(last_name)
    print('full name  --- > '+full_name)
    predicted_ethnicity_full, probablities_full = classifier.predict(full_name.upper())
    print('full-->'+str(probablities_full))
    if 'true' in booleanflag:
        result['probability_fullname'] = '0.914866'
    else:
        result['probability_fullname']=''.join(str(probablities_full[1]))
    #result['predicted_ethnicity_full'] = str(predicted_ethnicity_full)
    if first_name == "":
        predicted_ethnicity_first = -1
        probablities_first = -1
    else:
        predicted_ethnicity_first, probablities_first = classifier.predict(str(first_name).upper())
    if (first_name != ""):
        if 'true' in prefixflag:
            result['probability_first'] = '0'
        else:
            result['probability_first'] = ''.join(str(probablities_first[1]))
        #result['predicted_ethnicity_first'] = str(predicted_ethnicity_first)
    else:
        result['probability_first'] = "-1"
        #result['predicted_ethnicity_first'] = -1
    if last_name != "":
        predicted_ethnicity_last, probablities_last = classifier.predict(str(last_name).upper())
    if(last_name != ""):
        if 'true' in booleanflag:
            result['probability_last'] = '0.91894866'
        elif zeroflag==1:
            result['probability_last'] = '0'
        else:
            result['probability_last'] = ''.join(str(probablities_last[1]))
        #result['predicted_ethnicity_last'] = str(predicted_ethnicity_last)
    else:
        result['probability_last'] = "-1"
        #result['predicted_ethnicity_last'] = -1


    return result

if __name__ == '__main__':
    genderobj = GenderClassifier()
    #context = ('/tmp/server.crt', '/tmp/server.key')
    app.run(host='192.168.2.59', port=5000)


