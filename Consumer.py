#!/bin/sh
# !/var/mail/flask python
# coding: utf-8
import flask
import pickle
from io import StringIO,BytesIO
import ast
import datetime
import time
import os
import logging.config
import numpy as np
import spacy
from flask import request, jsonify, render_template, flash, redirect, json
from numpy.distutils.system_info import accelerate_info
from werkzeug.utils import secure_filename
from confluent_kafka import Consumer,KafkaException
import threading, base64

from PIL import Image, ImageSequence
from io import BytesIO
import pytesseract
import re
import array as arr
from psycopg2.extras import RealDictCursor
import psycopg2
import OCRUtils
from DocumentClasssifyModel import DocumentClassifier
import KafkaLogs, Logging
from KafkaLogs import connect
from flask import Flask
from OCRUtils import OCR
from flask import send_file
import requests
import re
import matplotlib.pyplot as plt
import logging
from PIL import Image
from ExtractPatientInfo import extract_patient_info
import itertools


app = Flask(__name__)
#context = SSL.Context(SSL.SSLv23_METHOD)
app = flask.Flask(__name__)
#app.config.from_object(__name__)
#app.config['SECRET_KEY'] = '7d441f27d441f27567d441f2b6176a'
#app.debug = True

logging.basicConfig(level=logging.DEBUG)
destdir = '/home/software/Pictures/'
Logging.configure_logging('/tmp/kafkalog')
logging.warn('Python has Started successfully')


# for entry in os.scandir(destdir):
#   if entry.is_file():




@app.route('/UpdateGlaceStatus', methods=['GET', 'POST'])
def updateGlace():
    if request.method == 'POST':

        content = request.data
        logging.info(str(content))
        jsonvalue = json.loads(content)
        patientname = jsonvalue["patientname"]
        classcategory = jsonvalue["classcategory"]
        classtype = jsonvalue["classtype"]
        faxID=jsonvalue["faxID"]
        accountId=jsonvalue["accountId"]
        insertQuery='insert into glace_response_data (glace_response_data_json) values ($$'+json.dumps(jsonvalue)+'$$)';
        insertData=(json.dumps(jsonvalue))
        classmapCount='select classtype_glace_mapping_id,predicted_class from classtype_glace_mapping where predicted_class ilike  (select image_processing_class from image_processing where image_processing_faxid = '+str(faxID).replace('\'', '')[1:-1]+' and image_processing_accountid ilike \''+str(accountId).replace('\'', '')[1:-1]+'\' ) and account_id ilike \''+str(accountId).replace('\'', '')[1:-1]+'\' and associated_class ilike \'%'+str(classtype).replace('\'', '')[1:-1]+'%\' limit 1';
        updateMappingCount='';
        logging.info(classmapCount)
        query='update image_processing set image_processing_glace_category= %s , image_processing_glace_class =%s ,image_processing_glace_patname=%s, image_processing_glace_flag=true where image_processing_faxid='+str(faxID).replace('\'', '')[1:-1]
        data = ( str(classcategory).replace('\'', '')[1:-1], str(classtype).replace('\'', '')[1:-1],str(patientname).replace('\'', '')[1:-1] )
    try:
        logging.info(query)
        kfdb = KafkaLogs.connect()
        cur = kfdb.cursor()
        cur.execute(insertQuery, insertData);
        logging.info('Glace Response Inserted successfully')
        logging.info('classmapCount  - > '+str(classmapCount))
        cur.execute(query, data);
        cur.execute(classmapCount);
        classmapcou=cur.fetchone()
        logging.info('classmapcou-- > '+str(classmapcou))
        if(classmapcou!=None) :
            logging.info('Data exist in NLP');
            #update faxIDs
            getFaxIDs="select classtype_glace_mapping_id,associated_faxids from classtype_glace_mapping where predicted_class ilike  '"+str(classmapcou[1])+"'  and account_id='"+str(accountId).replace('\'', '')[1:-1]+"'  and associated_class ilike '"+str(classtype).replace('\'', '')[1:-1]+"'";
            logging.info(getFaxIDs)
            cur.execute(getFaxIDs);
            associated_faxids = cur.fetchone();
            logging.info(str(associated_faxids))
            cur.execute("update classtype_glace_mapping set associated_faxids='"+str(associated_faxids[1])+","+str(faxID).replace('\'', '')[1:-1]+"' where classtype_glace_mapping_id="+str(associated_faxids[0])+" ")

            #updating associated count
            cur.execute('update classtype_glace_mapping set associated_count= (select associated_count +1 from classtype_glace_mapping where classtype_glace_mapping_id = '+str(classmapcou[0])+') where classtype_glace_mapping_id= '+str(classmapcou[0]));
            # updating total count
            cur.execute('update classtype_glace_mapping set total_count= (select sum(associated_count) from classtype_glace_mapping where predicted_class ilike  \''+str(classmapcou[1])+'\' and account_id ilike \''+str(accountId).replace('\'', '')[1:-1]+'\' ) where  predicted_class ilike  \''+str(classmapcou[1])+'\' and account_id ilike \''+str(accountId).replace('\'', '')[1:-1]+'\'')
            logging.info('update classtype_glace_mapping set total_count= (select sum(associated_count) from classtype_glace_mapping where predicted_class ilike  \''+str(classmapcou[1])+'\' and account_id ilike \''+str(accountId).replace('\'', '')[1:-1]+'\' ) where  predicted_class ilike  \''+str(classmapcou[1])+'\' and account_id ilike \''+str(accountId).replace('\'', '')[1:-1]+'\'')
            #update percentage for class mapping
            cur.execute('update classtype_glace_mapping set probability = (associated_count*100::decimal/total_count)');
        else :
            predicted_labelQry='select image_processing_class from image_processing where image_processing_faxid = '+str(faxID).replace('\'', '')[1:-1] +' and image_processing_accountid ilike \''+str(accountId).replace('\'', '')[1:-1]+'\'';
            logging.info(predicted_labelQry)
            cur.execute(predicted_labelQry);
            predict_label =cur.fetchone();
            logging.info(str(predict_label[0]))
            getCountQry='select coalesce(max(total_count)+1,1) from classtype_glace_mapping where predicted_class=\''+predict_label[0]+ '\'';
            cur.execute(getCountQry);
            max_total_count=cur.fetchone();
            insertmappingQry = 'insert into classtype_glace_mapping (account_id,predicted_class,associated_class,associated_count,total_count,probability,associated_faxids ) values ( %s ,%s,%s,%s,%s,%s,%s)';
            insertmappingData = (str(accountId).replace('\'', '')[1:-1],predict_label[0],str(classtype).replace('\'', '')[1:-1],1,max_total_count[0],100.0,str(faxID).replace('\'', '')[1:-1]);
            cur.execute(insertmappingQry,insertmappingData)
            logging.info('update classtype_glace_mapping set total_count = '+max_total_count[0]+' where predicted_class ilike \''+predict_label[0]+ '\'');
            #cur.execute('update classtype_glace_mapping set total_count = '+max_total_count[0]+' where predicted_class ilike \''+predict_label[0]+ '\'');
            cur.execute('update classtype_glace_mapping set total_count = (select sum(associated_count) from classtype_glace_mapping where predicted_class ilike \''+predict_label[0]+ '\' and account_id ilike \''+str(accountId).replace('\'', '')[1:-1]+'\'   ) where predicted_class ilike \''+predict_label[0]+ '\' and account_id ilike \''+str(accountId).replace('\'', '')[1:-1]+'\'' );
            cur.execute('update classtype_glace_mapping set probability = (associated_count*100::decimal/total_count)');







    except (Exception) as error:
        print(error)
        logging.warn(error)
        return str('Updated failue %s' +error);
    kfdb.commit()
    kfdb.close()




    return str('Updated Successfully');


@app.route('/', methods=['GET', 'POST'])
def home():


    if request.method == 'GET':
        r = requests.get(url='https://sso.glaceemr.com/TestSSOAccess?accountId=developmentr16' )

        return '''<h1>Welcome to Python API</h1>
<p>API is for FAX Processing</p>'''
    if request.method == 'POST':
        return "Welcome to Python POST API";




@app.route('/api/fax/all', methods=['GET', 'POST'])
def api_all():
    if request.method == 'GET':
        faxmodel = DocumentClassifier()
        faxmodel.predict_category('test', '')
        return jsonify('')
    if request.method == 'POST':

        consumetime = datetime.datetime.now()
        JSONImage=[]
        content = request.data
        # print(content)
        jsonvalue = json.loads(content)
        accoutId = jsonvalue["accountId"]
        faxId = jsonvalue["faxid"]
        base64EncodeString = jsonvalue["base64Image"]

        fullcontent=[]
        image_content=[]
        count=0
        path = 'static/'+datetime.datetime.now().strftime("%Y-%m-%d")
        print(datetime.datetime.now().strftime("%Y-%m-%d"))
        try:
            if not os.path.exists(path):
                os.mkdir(path)
                os.chmod(path, '0o777')
        except OSError:
            print("Creation of the directory %s failed" % path)
        else:
            print("Successfully created the directory %s " % path)
        for images in base64EncodeString:
            # now song is a dictionary
            count=count+1
            length = len(images)
            for attribute, value in images.items():
                # print (value)  # example usage
                imgdata = base64.b64decode(value)
                image = Image.open(BytesIO(imgdata))
                image.save(path+'/'+str(faxId)+'_'+accoutId+'_'+str(count)+'.png', "PNG")
                fileImagepaths={}
                fileImagepaths['base64Image']=path+'/'+str(faxId)+'_'+accoutId+'_'+str(count)+'.png'
                JSONImage.append(str(fileImagepaths))
                # mager = np.array(image)          # im2arr.shape: height x width x channel
                # image = Image.fromarray(imager)
                x = OCR()
                print('fax--->'+faxId)
                print(len(images))
                image_content.extend(x.convert_image_to_text(image=image))

                fullcontent=image_content


        #print(str(JSONImage).replace('\"', ''))
        faxmodel = DocumentClassifier()
        myarray = np.asarray(fullcontent)

        prediction = faxmodel.predict_category(fullcontent)

        patient_info = dict()
        print('coverpage : '+str(prediction["is_cover_page"]))


        imagecontent=''
        str1 = ''.join(str(e) for e in fullcontent)
        logging.info(str(str1))
        if prediction["is_cover_page"] == 1:
            if len(fullcontent) > 1:
                patient_info = extract_patient_info(str(str1),prediction["predicted_class_label"],patient_name_extraction_model)
            else:
                patient_info = extract_patient_info(str(str1),prediction["predicted_class_label"],patient_name_extraction_model)
        else:
            patient_info = extract_patient_info(str(str1),prediction["predicted_class_label"],patient_name_extraction_model)

        json_dump = json.dumps(str(prediction))
        jsonresult = json.loads(json_dump.replace('\'', '\"')[1:-1])
        logging.info('patient info -- > '+str(patient_info))
        patientinfo = json.loads(str(patient_info).replace('\'', '\"'))
        pateintname = patientinfo['names']
        result = dict()
        print('name : ' + str(patientinfo['names']))
        print('dob :' + patientinfo['dob'])
        prediction['namearray']= str(patientinfo['names'])
        prediction['dob'] = patientinfo['dob']
        prediction['faxID'] =faxId
        print('data: '+str(prediction))

        query = "insert into image_processing (image_processing_accountid,image_processing_faxid,image_processing_base64,image_processing_class,image_processing_coverpage,image_processing_json,image_processing_patientname,patint_dob,predicted_class_confidence,image_processing_ocrtext) VALUES (%s, %s, %s, %s ,%s, %s , %s , %s , %s , %s);";
        data = (
        accoutId, faxId, str(JSONImage).replace('\"', ''), jsonresult['predicted_class_label'], str(jsonresult['is_cover_page']),
        json.dumps(jsonresult), str(patientinfo['names']), patientinfo['dob'],
        str(jsonresult['predicted_class_confidence']),str(fullcontent).encode('ascii', 'ignore').decode('ascii'))
        try:
            kfdb = KafkaLogs.connect()
            cur = kfdb.cursor()
            cur.execute(query, data);
        except (Exception) as error:
            print(error)
            logging.warn(error)
        kfdb.commit()
        kfdb.close()
        processedtime = datetime.datetime.now()
        elapsedTime = processedtime - consumetime
        print('days dif ' + str(elapsedTime))

        #print('min diff' + elapsedTime)
        print('Inserted Successfully')
        print('Inserted Successfully')
        logging.info(str(datetime.datetime.now()))
        print(prediction)
        return jsonify(str(prediction))





@app.route('/download')
def downloadFile ():
    #For windows you need to use drive name [ex: F:/Example.pdf]
    if 'fromdate' in request.args:
        fromdate= str(request.args['fromdate'])
    if 'todate' in request.args:
        todate= str(request.args['todate'])
    if 'mode' in request.args:
        mode = str(request.args['mode'])

    try:
        kfdb = KafkaLogs.connect()
        cur = kfdb.cursor()
        print(mode)
        if mode=='1':
            cur.execute("select string_agg(associated_faxids,',') from classtype_glace_mapping where markedcolumn is true")
            faxIDs=cur.fetchone();
            query="select json_agg(t) from (select image_processing_coverpage as iscover,image_processing_ocrtext as content,markedclass as changedclass,image_processing_page_count as pagecount,image_processing_accountid as accountid,image_processing_faxid as faxid from image_processing where image_processing_faxid in  ("+faxIDs[0]+"))t"

        else:
            query = "select json_agg(t) from ( select image_processing_accountid as AccountID,image_processing_class as CorrectedClass,image_processing_json->>'predicted_class_label' as predictedClass, image_processing_patientname as PatientName,image_processing_coverpage as isCover,image_processing_faxid as FaxID,patint_dob as DOB, image_processing_ocrtext as Content from image_processing where image_verified = 1 and created_on::date >= timestamp '" + fromdate + "' and created_on::date <= timestamp '" + todate + "')t"
        logging.info(query)
        cur.execute(query)
        data = cur.fetchall()
        try:
            if not os.path.exists('/tmp/data.json'):
                os.mkdir('/tmp/data.json')
        except OSError:
            print("Creation of the datajson " )
        else:
            print("Successfully created the datajson  " )
        with open('/tmp/data.json', 'w') as outfile:
             json.dump(data, outfile)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logging.warn(error)
        kfdb.commit()
        kfdb.close()
    path = "/tmp/data.json"
    return send_file(path, as_attachment=True)

@app.route('/FeedbackStatus', methods=['GET'])
def google_pie_chart():
    kfdb = KafkaLogs.connect()
    cur = kfdb.cursor()
    cur.execute("select account_id,predicted_class,associated_class,total_count,associated_count,CAST (probability AS FLOAT),coalesce(associated_faxids,''),classtype_glace_mapping_id from classtype_glace_mapping order by account_id");
    data = cur.fetchall()
    print(data)
    return render_template('feedback.html', data=str(json.dumps(data)))

@app.route('/index', methods=['GET'])
def index():
    global data,jsonarray,totalpre,totalname,totalclass,jsonobj,jsonobj2,jsonobj3,jsonobj4,totalcover,query,verified,faxid,patname,patstatus,classname,classstatus,cover,coverstatus

    try:
        kfdb = KafkaLogs.connect()
        cur = kfdb.cursor()

        query='update image_processing set '

        if 'faxid' in request.args:
            faxid=  str(request.args['faxid'])
        if 'verified' in request.args:
            verified= str(request.args['verified'])

            if 'true' in verified:
                cur.execute('update image_processing set image_verified=1,predict_default=1,image_processing_patientname_status=1,image_processing_class_status=1,image_processing_coverpage_status=1 where image_processing_faxid='+faxid)
                kfdb.commit()
                kfdb.close()
            elif 'false' in verified:
                query=query+' image_verified=1, '
            if 'patname' in request.args:
                patname = str(request.args['patname'])
                query = query + 'image_processing_patientname = \''+patname+'\' ,'
            if 'patstatus' in request.args:
                patstatus = str(request.args['patstatus'])
                query = query + 'image_processing_patientname_status ='+patstatus+' ,'
            if 'classname' in request.args:
                classname = str(request.args['classname'])
                query = query + 'image_processing_class = \''+classname+'\' ,'
            if 'classstatus' in request.args:
                classstatus = str(request.args['classstatus'])
                query = query + 'image_processing_class_status = '+classstatus+' ,'
            if 'cover' in request.args:
                cover = str(request.args['cover'])
                query = query + 'image_processing_coverpage = '+cover+' ,'
            if 'coverstatus' in request.args:
                coverstatus = str(request.args['coverstatus'])
                query = query + 'image_processing_coverpage_status ='+coverstatus+' ,'
            query=query.rstrip(',')

            query =query+ ' where image_processing_faxid= '+faxid+' ;'
            kfdb = KafkaLogs.connect()
            cur = kfdb.cursor()


            print(query)
            cur.execute(query)
            result = cur.rowcount
            print(result)
            kfdb.commit()
            kfdb.close()
            print('Updated successfully : '+query
              )

    except (Exception,psycopg2.DatabaseError) as error:
        print(error)
        logging.warn(error)
        kfdb.commit()
        kfdb.close()




    try:
        kfdb = KafkaLogs.connect()
        cur = kfdb.cursor(cursor_factory=RealDictCursor)
        if 'mode' in request.args:
            print(str(request.args['mode']))
            if '1' in str(request.args['mode']):
                if 'date' in request.args:
                    cur.execute('select image_processing_faxid as faxid,predicted_class_confidence as confidence,created_on as date, image_processing_patientname as patientname,image_processing_accountid as accountid, image_processing_class as class,image_processing_coverpage as coverpage,image_processing_base64 as base64, patint_dob as dob,image_process_id as id,image_verified as verify,image_processing_time as processtime from image_processing where created_on::date = \''+str(request.args['date'])+'\'::date and image_verified = 1;');

                elif 'from' in request.args and 'to' in request.args:
                    cur.execute('select image_processing_faxid as faxid,predicted_class_confidence as confidence,created_on as date, image_processing_patientname as patientname,image_processing_accountid as accountid, image_processing_class as class,image_processing_coverpage as coverpage,image_processing_base64 as base64, patint_dob as dob,image_process_id as id,image_verified as verify,image_processing_time as processtime from image_processing where created_on::date > \'' + str(request.args['from']) + '\'::date and created_on::date <= \'' + str(request.args['to']) + '\'::date and image_verified = 1;');
                else:
                    cur.execute('select image_processing_faxid as faxid,predicted_class_confidence as confidence,created_on as date, image_processing_patientname as patientname,image_processing_accountid as accountid, image_processing_class as class,image_processing_coverpage as coverpage,image_processing_base64 as base64, patint_dob as dob,image_process_id as id,image_verified as verify,image_processing_time as processtime from image_processing where created_on::date = now()::date and image_verified = 1;');
            elif '2' in str(request.args['mode']):
                cur.execute('select image_processing_faxid as faxid,predicted_class_confidence as confidence,created_on as date, image_processing_patientname as patientname,image_processing_accountid as accountid, image_processing_class as class,image_processing_coverpage as coverpage,image_processing_base64 as base64, patint_dob as dob,image_process_id as id,image_verified as verify,image_processing_time as processtime from image_processing where image_verified = 0;');
            else :
                if 'date' in request.args:
                    cur.execute('select image_processing_faxid as faxid,predicted_class_confidence as confidence,created_on as date, image_processing_patientname as patientname,image_processing_accountid as accountid, image_processing_class as class,image_processing_coverpage as coverpage,image_processing_base64 as base64, patint_dob as dob,image_process_id as id,image_verified as verify,image_processing_time as processtime from image_processing where created_on::date = \''+str(request.args['date'])+'\'::date and image_verified = 0;');
                elif 'from' in request.args and 'to' in request.args:
                    cur.execute('select image_processing_faxid as faxid,predicted_class_confidence as confidence,created_on as date, image_processing_patientname as patientname,image_processing_accountid as accountid, image_processing_class as class,image_processing_coverpage as coverpage,image_processing_base64 as base64, patint_dob as dob,image_process_id as id,image_verified as verify,image_processing_time as processtime from image_processing where created_on::date > \'' + str(request.args['from']) + '\'::date and created_on::date <= \'' + str(request.args['to']) + '\'::date and image_verified = 0;');

                else:
                    cur.execute('select image_processing_faxid as faxid,predicted_class_confidence as confidence,created_on as date, image_processing_patientname as patientname,image_processing_accountid as accountid, image_processing_class as class,image_processing_coverpage as coverpage,image_processing_base64 as base64, patint_dob as dob,image_process_id as id,image_verified as verify,image_processing_time as processtime from image_processing where created_on::date = now()::date and image_verified = 0;');

        else:
            cur.execute('select image_processing_faxid as faxid,predicted_class_confidence as confidence,created_on as date, image_processing_patientname as patientname,image_processing_accountid as accountid, image_processing_class as class,image_processing_coverpage as coverpage,image_processing_base64 as base64, patint_dob as dob,image_process_id as id,image_verified as verify,image_processing_time as processtime from image_processing where created_on::date = now()::date and image_verified = 0;');
        data=cur.fetchall()

        cur.execute('select count(*) as total from image_processing where image_verified=1;')
        totalpre=cur.fetchall()
        cur.execute('select count(*) as totalname from image_processing where image_processing_patientname_status=1;')
        totalname = cur.fetchall()
        cur.execute('select count(*) as totalclass from image_processing where image_processing_class_status=1;')
        totalclass = cur.fetchall()
        cur.execute('select count(*) as totalcover from image_processing where image_processing_coverpage_status=1;')
        totalcover = cur.fetchall()
        jsonobj={}
        jsonobj2={}
        jsonobj3={}
        jsonobj4 = {}
        jsonobj=totalpre
        jsonobj2=totalname
        jsonobj3=totalclass
        jsonobj4 = totalcover
        print(totalcover)

        #print(str(json.dumps(jsonarray)))


    except (Exception) as error:
        print(error)
        logging.warn(error)
    kfdb.close()
    # return json.dumps(data)


    #print(str(json.dumps(data)))
    return render_template('index.html',obj=jsonobj, totalname=jsonobj2, obj2=jsonobj3, totalcover=jsonobj4, data=str(json.dumps(data)))
@app.route('/getFaxFilenames', methods=['GET'])
def getFileNames():
    if 'faxid' in request.args:
        faxid = str(request.args['faxid'])
    try:
        kfdb = KafkaLogs.connect()
        cur = kfdb.cursor(cursor_factory=RealDictCursor)
        cur.execute('select image_processing_base64 as base64 from image_processing where image_processing_faxid ='+faxid);
        data = cur.fetchone()



    except (Exception) as error:
        print(error)

    kfdb.close()

    return json.dumps(data);


@app.route('/markRecord', methods=['GET'])
def markRecord():
    try:
        kfdb = KafkaLogs.connect()
        cur = kfdb.cursor()
        cur = kfdb.cursor()
        if 'classID' in request.args:
            classid = str(request.args['classID'])
        if 'markedClass' in request.args:
            markedClass = str(request.args['markedClass'])
        query = "update classtype_glace_mapping set markedcolumn =true ,markedclass= '"+markedClass+"' where classtype_glace_mapping_id="+classid;
        getFaxIDS= "select associated_faxids from classtype_glace_mapping where classtype_glace_mapping_id ="+classid;
        cur.execute(getFaxIDS)
        data=cur.fetchone();
        updateImage_process_marked="update image_processing set markedclass='"+markedClass+"' where image_processing_faxid in ("+data[0]+")"
        print(updateImage_process_marked)
        cur.execute(updateImage_process_marked)
        cur.execute(query)
        kfdb.commit()
        kfdb.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logging.warn(error)
        kfdb.commit()
        kfdb.close()
        return '0'
    return '1'

@app.route('/updateRecord', methods=['GET'])
def updaterecord():
    try:
        kfdb = KafkaLogs.connect()
        cur = kfdb.cursor()

        query = 'update image_processing set '

        if 'faxid' in request.args:
            faxid = str(request.args['faxid'])
        if 'verified' in request.args:
            verified = str(request.args['verified'])
            print('verify -->'+verified)
            if 'true' in verified:
                cur.execute(
                    'update image_processing set image_verified=1,predict_default=1,image_processing_patientname_status=1,image_processing_class_status=1,image_processing_coverpage_status=1 where image_processing_faxid=' + faxid)
                kfdb.commit()
                kfdb.close()
                return '1'
            elif 'false' in verified:
                query = query + ' image_verified=1, '
            if 'patname' in request.args:
                patname = str(request.args['patname'])
                query = query + 'image_processing_patientname = \'' + patname + '\' ,'
            if 'patstatus' in request.args:
                patstatus = str(request.args['patstatus'])
                query = query + 'image_processing_patientname_status =' + patstatus + ' ,'
            if 'classname' in request.args:
                classname = str(request.args['classname'])
                query = query + 'image_processing_class = \'' + classname + '\' ,'
            if 'classstatus' in request.args:
                classstatus = str(request.args['classstatus'])
                query = query + 'image_processing_class_status = ' + classstatus + ' ,'
            if 'cover' in request.args:
                cover = str(request.args['cover'])
                query = query + 'image_processing_coverpage = ' + cover + ' ,'
            if 'coverstatus' in request.args:
                coverstatus = str(request.args['coverstatus'])
                query = query + 'image_processing_coverpage_status =' + coverstatus + ' ,'
            query = query.rstrip(',')

            query = query + ' where image_processing_faxid= ' + faxid + ' ;'
            kfdb = KafkaLogs.connect()
            cur = kfdb.cursor()

            print('query --> '+query)
            cur.execute(query)
            result = cur.rowcount
            print(result)
            kfdb.commit()
            kfdb.close()
            print('Updated successfully : ' + query
                  )

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logging.warn(error)
        kfdb.commit()
        kfdb.close()
        return '0'
    return '1'

class ConsumerThread(threading.Thread):

    def __init__(self, bootstrapserver, groupid, offset, topic, id):
        super(ConsumerThread, self).__init__()
        self.bootstrapserver = bootstrapserver
        self.groupid = groupid
        self.offset = offset
        self.topic = topic
        self.id = id

    def run(self):
        c = Consumer({
            'bootstrap.servers': self.bootstrapserver,
            'group.id': self.groupid,
            #'security.protocol': 'SASL_SSL',
            'auto.offset.reset': 'earliest',
            #'session.timeout.ms': 60000,
            #'max.poll.interval.ms': 60000
            #'ssl.ca.location': '/var/ssl/kafka-server.pem',
            # CA certificate file for verifying the broker's certificate.
            #'ssl.certificate.location': '/var/ssl/glacemain.pem',  # Client's certificate
            #'ssl.key.location': '/var/ssl/glaceprivate.key',  # Client's key
            #'ssl.key.password': 'Glenwood100*',  # Key password, if any.
            #'sasl.mechanisms': 'SCRAM-SHA-512',
            #'sasl.username': 'admin',
            #'sasl.password': 'admin',
            #'debug': 'all',
            #'auto.commit.interval.ms': 5000,
            #'enable.auto.offset.store': True,
            #'retries': 2,
            #'default.topic.config': {'auto.offset.reset': 'earliest'},
            #'debug': 'protocol,security',
            #'max.partition.fetch.bytes':1048576

        })

        c.subscribe([self.topic])

        try:
            while True:
                c.subscribe([self.topic])
                msg = c.poll(1.0)
                if msg is None:
                   # logging.debug("   @@@@@@@@@    POLL TIMED OUT   @@@@@@@@@")
                    continue
                if msg.error():
                    logging.info("Consumer error: {}".format(msg.error()))
                    error_code = msg.error().code()

                    if error_code == confluent_kafka.KafkaError._PARTITION_EOF:
                        continue
                    continue
                x = datetime.datetime.now()
                fmt = '%Y-%m-%d %H:%M:%S'
                consumetime = datetime.datetime.now()
                logging.info("Message received by  << >>" + str(self.id) + str(x))
                JSONImage = []
                jsonvalue = json.loads(msg.value().decode('utf-8'))


                accoutId = jsonvalue["accountId"]
                faxId = jsonvalue["faxid"]
                base64EncodeString = jsonvalue["base64Image"]
                try:
                    kfdb = KafkaLogs.connect()
                    cur = kfdb.cursor()
                    mainInsert = 'insert into consumer_data (consumer_data_accountid,consumer_data_faxid,consumer_data_json) values (' + str(accoutId )+','+str(faxId)+',$$'+json.dumps(jsonvalue)+'$$)';
                    cur.execute(mainInsert);
                    kfdb.commit()
                    kfdb.close()
                except (Exception) as error:
                    print(error)
                    logging.warn(error)
                kfdb.commit()
                kfdb.close()

                logging.info('Inserted Main data Successfully')
                fullcontent = []
                image_content = []
                count = 0
                path = 'static/' + datetime.datetime.now().strftime("%Y-%m-%d")
                print(datetime.datetime.now().strftime("%Y-%m-%d"))
                try:
                    if not os.path.exists(path):
                        os.mkdir(path)
                except OSError:
                    print("Creation of the directory %s failed" % path)
                else:
                    print("Successfully created the directory %s " % path)
                for images in base64EncodeString:
                # now song is a dictionary
                    count = count + 1
                    length = len(images)
                    for attribute, value in images.items():
                    # print (value)  # example usage
                        imgdata = base64.b64decode(value)
                        image = Image.open(BytesIO(imgdata))
                        image.save(path + '/' + str(faxId) + '_' + accoutId + '_' + str(count) + '.png', "PNG")
                        fileImagepaths = {}
                        fileImagepaths['base64Image'] = path + '/' + str(faxId) + '_' + accoutId + '_' + str(count) + '.png'
                        JSONImage.append(str(fileImagepaths))
                    # mager = np.array(image)          # im2arr.shape: height x width x channel
                    # image = Image.fromarray(imager)
                    #x = OCR()
                        print(len(images))
                        image_content.extend(ocrobject.convert_image_to_text(image=image))

                        fullcontent = image_content

                print(str(JSONImage).replace('\"', ''))


                print(len(fullcontent))
                myarray = np.asarray(fullcontent)

                prediction = faxmodel.predict_category(fullcontent,accoutId)

                patient_info = dict()
                print('coverpage : ' + str(prediction["is_cover_page"]))
                mappingClassQry = 'select associated_class from classtype_glace_mapping where probability=(select max(probability) from classtype_glace_mapping where predicted_class ilike lower(\''+prediction["predicted_class_label"]+'\') )and account_id ilike \''+accoutId+'\' and total_count > 15  limit 1';
                kfdb = KafkaLogs.connect()
                cur = kfdb.cursor()
                cur.execute(mappingClassQry);
                class_type = cur.fetchone();
                if(class_type !=None) :
                    logging.info('Mapped Class type  -- > ')

                    logging.info('class type -- > '+str(class_type))
                    prediction["predicted_class_label"]=str(class_type);
                kfdb.commit()
                kfdb.close()
                print(type(fullcontent))
                if prediction["is_cover_page"] == 1:
                    str1 = ''.join(str(e) for e in fullcontent[1:])
                else:
                    str1 = ''.join(str(e) for e in fullcontent)
                logging.info(str(str1))
                if prediction["is_cover_page"] == 1:
                    if len(fullcontent) > 1:
                        patient_info = extract_patient_info(str(str1),prediction["predicted_class_label"],patient_name_extraction_model)
                    else:
                        patient_info = extract_patient_info(str(str1),prediction["predicted_class_label"],patient_name_extraction_model)
                else:
                    patient_info = extract_patient_info(str(str1),prediction["predicted_class_label"],patient_name_extraction_model)

                json_dump = json.dumps(str(prediction))
                jsonresult=json.loads(json_dump.replace('\'','\"')[1:-1])
                patientinfo= json.loads(str(patient_info).replace('\',','\",'))
                pateintname=patientinfo['names']

                print('name : '+str(patientinfo['names']))
                print('dob :' +patientinfo['dob'])
                myarray2 = np.asarray(str1)
                final = str(myarray2.tolist()).encode('ascii', 'ignore').decode('ascii')
                processedtime = datetime.datetime.now()
                elapsedTime = processedtime - consumetime
                print('days dif ' + str(elapsedTime))

                print(
                    "Processing Received message: {} " + str(jsonresult['is_cover_page'])  +' : '+ jsonresult['predicted_class_label'])
                query="insert into image_processing (image_processing_accountid,image_processing_faxid,image_processing_base64,image_processing_class,image_processing_coverpage,image_processing_json,image_processing_patientname,patint_dob,predicted_class_confidence,image_processing_ocrtext) VALUES (%s, %s, %s, %s ,%s, %s , %s , %s , %s , %s, %s);";
                data=(accoutId,faxId,str(JSONImage).replace('\"', ''),jsonresult['predicted_class_name'],str(jsonresult['is_cover_page']),json.dumps(jsonresult),str(patientinfo['names']),patientinfo['dob'],str(jsonresult['predicted_class_confidence']),str(final).replace('\'','\\\''),str(elapsedTime))
            #print(base64EncodeString)
                try:
                    logging.info(tracker.print_diff())
                    kfdb = KafkaLogs.connect()
                    cur=kfdb.cursor()
                    cur.execute(query,data);
                    kfdb.commit()
                    kfdb.close()
                except (Exception) as error:
                    print(error)
                    logging.warn(error)
                kfdb.commit()
                kfdb.close()

                print('Inserted Successfully')

                prediction['namearray'] = str(patientinfo['names']).replace('\'','@@')
                newdob=re.findall(r'\d+', patientinfo['dob'])
                logging.info('newdob--->   ************************* '+ newdob.replace('[\'','').replace('\']',''))
                #logging.info('updated dob'+str(patientinfo['dob'])
                try:
                    date_of_birth = datetime.datetime.strptime(str(patientinfo['dob']), "%m/%d/%Y")
                    prediction['dob'] = str(patientinfo['dob'])
                except:
                    logging.info('Incorrect Date -- > '+str(patientinfo['dob']) )
                    prediction['dob'] = ''

                prediction['faxID'] = faxId
                prediction = str(prediction).replace('\'', '\"')
                prediction = str(prediction).replace('@@', '\'')
                print('sending to Glace -- > '+str(prediction))
                r = requests.get(url='https://sso.glaceemr.com/TestSSOAccess?accountId='+accoutId)

                data = r.json()
                accountUrl = data['Glace_tomcat_URL'] + '/SaveFaxDetails'
                accountUrl=accountUrl.replace('http','https')
                print('sso Url -- > ' + accountUrl.replace('http','htpps'))
                try:
                    r = requests.post(url=accountUrl, data=str(prediction).encode('utf-8'))
                except (Exception) as error:
                    logging.info('***************Exception in connecting server ******************'+ str(accountUrl))
                    continue;
            # extracting response text
                responsefromleagcy = r.text
                print("The GlaceReponse is:%s" % responsefromleagcy)

                logging.info('response code-- >'+r.status_code)


                logging.info(str(datetime.datetime.now()))

        except KafkaException as ex:
            logging.error(ex)
            time.sleep(2)
            #c.commit(async=False)
            logging.info('Exception < -- *********Consumer closed successfully*********** -- > ')
            c.close()

        finally:
            # Close down consumer to commit final offsets.
            #c.commit(async=False)
            logging.info('< -- *********Consumer closed successfully*********** -- > ')
            c.close()


if __name__ == '__main__':
    NO_OF_CONSUMERS = 2;
    bootstrapserver = '127.0.0.1:9092'
    groupid = 'ocr_process'
    offset = 'earliest'
    topic = 'ocr'

    for i in range(NO_OF_CONSUMERS):
        mythread = ConsumerThread(bootstrapserver, groupid, offset, topic, i)
        mythread.start()
    print('*******************INTIALIATION****************')
    ocrobject = OCR()
    faxmodel = DocumentClassifier()

    document_category_prediction_model = pickle.load(open("models/fax_classification_model.mod", "rb"))
    patient_name_extraction_model = spacy.load("patient_name_extraction_model")
    context = ('/var/ssl/glacepublic.cer', '/var/ssl/glaceprivate.key')
    app.run(host='192.168.2.59', port=1443)
