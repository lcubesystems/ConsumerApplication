import glob
import os
import pickle
from itertools import groupby

import pandas as pd
import os
from tqdm import tqdm,tqdm_notebook
import threading, time, random
import pickle
from PIL import Image
import pytesseract
import re

from PIL import Image, ImageSequence
from io import BytesIO
import numpy as np

import itertools
import datetime

import psycopg2

from DocumentClasssifyModel import CoverPagePredictor

import KafkaLogs

path = '/home/software/Documents/ML-fax-Model/NCA'

def converttoText():
    try:
        filedata=[]
        kfdb = KafkaLogs.connect()
        cur = kfdb.cursor()
        cur.execute(
            "select eaamdata_id , class_type,page_count,eaamdata_filenames from eaamdata where updated_flag is false");
        for mainfiledata in cur.fetchall():
            mainID=mainfiledata[0]
            class_type=mainfiledata[1]
            page_count=mainfiledata[2]
            filenames=mainfiledata[3]
            full_content_data=[]
            count=-1;flag=0
            for fileslist in filenames.split(','):
                count=count+1
                print(fileslist + ' : '+str(count))
                filedata=convert_text('/home/software/Documents/ML-fax-Model/NCA/'+fileslist)
                if "_1.png" in fileslist:
                    flag=count
                full_content_data.append(str(filedata))
                #print(str(filedata))


            print('new'+str(full_content_data))
            coverpage_model = pickle.load(open("cover_page_model.mod", "rb"))
            #print(filedata[0])
            df = pd.DataFrame(columns=["document"])
            df = df.append({"document": full_content_data[flag]}, ignore_index=True)
            print('flag-- > '+str(flag))
            is_cover_page = coverpage_model.predict(df["document"])[0]
            print('is_cover_page'+str(is_cover_page))
            test_data = []
            labels = pd.read_csv("labels.csv")
            query="update eaamdata set updated_flag=true,eaamdata_ocrtext=$$"+str(full_content_data).encode('ascii', 'ignore').decode('ascii').replace('$$','') + " $$ , iscoverpage= "+str(is_cover_page)+" where eaamdata_id="+str(mainID);
            print(query)

            cur.execute(query);
            kfdb.commit()




    except (Exception) as error:
        print(error)
    kfdb.commit()
    kfdb.close()

def convert_text(filename):
    im = Image.open(filename)
    temp = BytesIO()
    full_content=pytesseract.image_to_string(Image.open(filename))
    return full_content

def groupEMRData(type):
    try:

        kfdb = KafkaLogs.connect()
        cur = kfdb.cursor()
        cur.execute("select distinct filedetails_patientid from filename inner join filedetails on filename_scanid=filedetails_id where filedetails_categoryid in (select patient_doc_category_id from patient_doc_category where patient_doc_category_name=\'"+type+"\')  and filedetails_creationdate::date >='2019-06-01' and filename_name ilike '2019-%'");
        message=cur.fetchall()
        mainQuery=''
        for data in message:
            mainQuery='select string_agg(filename_name,\',\') as filename ,\''+type+'\' as Type,count(*),\'NCA\' as accountid,filename_scanid from filename inner join filedetails on filename_scanid=filedetails_id where filedetails_categoryid in (select patient_doc_category_id from patient_doc_category where patient_doc_category_name=\''+type+'\') and filedetails_creationdate::date >=\'2019-06-01\' and filename_name ilike \'2019-%\' and filedetails_patientid='+str(data[0]) +' group by  filename_scanid';
            try:
                cur.execute(mainQuery)
                totaldata=cur.fetchall()
                print(totaldata)
                for mainfiledata in totaldata:
                    print(str(mainfiledata[0]))
                    query = "insert into eaamdata (eaamdata_filenames,eaamdata_accountid,class_type,page_count) VALUES (%s, %s, %s, %s );";
                    data = (str(mainfiledata[0]), str(mainfiledata[3]), str(mainfiledata[1]),
                            str(mainfiledata[2]));
                    cur.execute(query,data)
                    print('Inserted Successfully ')
            except (Exception) as error:
                print(error)




    except (Exception) as error:
        print(error)
    kfdb.commit()
    kfdb.close()



def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        # connect to the PostgreSQL server
       # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("dbname='nlp' user='glace' host='192.168.2.3' password='glace'")

        # create a cursor
        cur = conn.cursor()
        return  conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def updateModelData():
    kfdb = connect()
    cur = kfdb.cursor()
    test_data = []
    cur.execute(
        "select image_processing_coverpage,image_processing_ocrtext,image_processing_class,image_processing_page_count,image_processing_accountid,image_processing_faxid from image_processing where image_processing_faxid in (37430,37432,37435,37436,37440,37155,37179,37719,37720,37721,37722,37599,37595) ;");
    totaldata = cur.fetchall()
    for data in totaldata:
        print(str(data[1]))
        try:
            doc_object = {}
            is_cover_page = data[0]
            print(str(data[1]).replace('\\n', '\n'))
            doc_object["is_cover_page"] = is_cover_page
            doc_object["pages"] = str(data[1]).replace('\\n', '\n')
            doc_object["class_name"] = 'Lab Results'
            doc_object["label"] = '31'
            doc_object["page_count"] = str(data[3])
            #doc_object["accountid"] = str(data[5])

            test_data.append(doc_object)
            print(doc_object)

        except Exception as e:
            print(e)

    pickle.dump(test_data, open("/home/software/Documents/ML-fax-Model/data_old/sbedi_labresults_" + str(datetime.date.today()) + ".p", "wb"))
def fetchPatientNameData():
    count = 0;
    kfdb = connect()
    cur = kfdb.cursor()
    test_data = []
    cur.execute(
        "select image_processing_ocrtext,split_part(split_part(image_processing_glace_patname,'@@',2),',',1), split_part(split_part(image_processing_glace_patname,'@@',2),',',2) from image_processing where image_processing_patientname is not null and image_processing_glace_patname is not null ");
    totaldata = cur.fetchall()
    first_name=''
    last_name = ''

    for data in totaldata:
        ocr_text=str(data[0]).replace('\n',' ').upper();
        #print(ocr_text)
        name_series=str(data[1]) + ', ' + str(data[2]).strip()
        name_series_two=str(data[2]) + ', ' + str(data[1]).strip()
        name_series_three=str(data[1]) + ' ' + str(data[2]).strip()
        name_series_four = str(data[2]) + ' ' + str(data[1]).strip()

        #print(ocr_text)
        #print(str(data[1]) + ',' + str(data[2]) + ' : '+ocr_text)
        if(data[1]!='' and data[2]!=''):
            if(name_series in ocr_text.upper() or  name_series_two in ocr_text.upper() or name_series_three in ocr_text.upper() or name_series_four in ocr_text.upper()):
                print(name_series + ' :: - > '+ocr_text)
                count=count+1
                doc_object = {}
                is_cover_page = data[0]
                print(str(data[1]).replace('\\n', '\n'))
                doc_object["content"] = ocr_text
                doc_object["patient_name"] = name_series

                # doc_object["accountid"] = str(data[5])

                test_data.append(doc_object)
                # print(test_data)

    pickle.dump(test_data,
                    open("/home/software/Documents/ML-fax-Model/data_old/name_extraction_" + str(datetime.date.today()) + ".p",
                         "wb"))



    print(count)
    #print(first_name)

def GeneratePatInfoJSON():
    count = 0;
    kfdb = connect()
    cur = kfdb.cursor()
    test_data = []
    faxIDlist=[88823,88825,88826,88827,88828,88829,88830,88831,88832,88833,88834,88835,88836,88837,88838,88839,88840,88841,88842,88843,88844,88845,88846,88847,88848,88849,88850,88851,88852,88853,88854,88855,88856,88857,88858,88859,88860,88861,88862,88863,88865,88866,88867,88868,88869,88870,88872,88873,88874,88875,88876,88877,88878,88879,88881,88882,88883,88884,88885,88886,88887,88888,88889,88890,88891,88892,88893,88894,88895,88896,88897,88898,88899,88900,88901,88902,88903,88904,88905,88907,88908,88909,88910,88911,88912,88913,88914,88915,88916,88917,88919,88921,88922,88923,88924,88925,88926,88927,88928,88929,88930,88931,88932,88933,88935,88936,88937,88938,88939,88940,88941,88942,88943,88944,88945,88947,88948,88949,88950,88951,88952,88953,88954,88955,88957,88958,88959,88960,88961,88962,88963,88964,88965,88966,88967,88968,88969,88970,88971,88972,88973,88974,88975,88976,88977,88978,88979,88980,88981,88982,88983,88984,88985,88986,88987,88988,88989,88990,88991,88992,88993,88994,88995,88996,89002,89003,89005,89006,89007,89008,89009,89010,89011,89012,89013,89014,89015,89016,89017,89018,89019,89020,89021,89022,89023,89024,89025,89026,89027,89028,89029,89030,89031,89032,89033,89034,89035,89036,89038,89039,89040,89041,89042,89044,89045,89046,89048,89049,89050,89051,89052,89053,89054,89055,89056,89057,89058,89059,89060,89061,89062,89063,89064,89066,89067,89068,89069,89070,89071,89072,89073,89074,89075,89076,89077,89078,89079,89080,89081,89082,89083,89084,89085,89086,89087,89088,89089,89090,89091,89093,89094,89095,89096,89097,89098,89099,89100,89101,89102,89103,89105,89106,89107,89108,89109,89110,89111,89113,89114,89115,89116,89117,89118,89119,89120,89121,89122,89123,89124,89125,89126,89127,89128,89129,89130,89131,89132,89133,89134,89135,89136,89137,89138,89139,89140,89141,89142,89143,89144,89145,89146,89147,89148,89149,89150,89151,89152,89153,89154,89155,89158,89159,89160,89161,89162,89163,89164,89165,89166,89167,89168,89169,89170,89171,89172,89173,89174,89176,89177,89178,89179,89180,89181,89182,89183,89184,89185]
    print(len(faxIDlist))

    for faxID in faxIDlist:
        print(faxID)
        cur.execute("select patient_registration_id,patient_registration_first_name,patient_registration_last_name,patient_registration_dob,patient_registration_sex,patient_registration_phone_no,patient_registration_address1,patient_registration_address2 ,emp_profile_fullname as pridoc from patient_registration  inner join emp_profile on patient_registration_principal_doctor=emp_profile_empid where patient_registration_id in (select filedetails_patientid from filedetails inner join filename on filedetails_id=filename_scanid where filename_name = (select a[1] from (select regexp_split_to_array(fax_inbox_filenames::text,',') from fax_inbox where fax_inbox_id in ("+str(faxID)+")) dt(a)))");
        totaldata = cur.fetchall()
        print(len(totaldata))
        if(len(totaldata)>0):
            for data in totaldata :
                doc_object = {}
                print('started')
                doc_object["patientid"]=str(data[0]).strip();
                doc_object["firstname"] = str(data[1]).strip();
                doc_object["lastname"]= str(data[2]).strip();
                doc_object["dob"] = str(data[3]).strip();
                doc_object["gender"] = str(data[4]).strip();
                doc_object["phoneno"] = str(data[5]).strip();
                doc_object["address1"] = str(data[6]).strip();
                doc_object["address2"]= str(data[7]).strip();
                doc_object["refdoctor"] = str(data[8]).strip();
                doc_object["accountid"] = 'anpatel';
                cur.execute("select image_processing_ocrtext from image_processing where image_processing_faxid=" + str(faxID) );
                ocrdata = cur.fetchone()
                doc_object["content"] = ocrdata;
                print(doc_object)
                test_data.append(doc_object)

                print(test_data)
                # print(test_data)

    pickle.dump(test_data,
                    open("/home/software/Documents/ML-fax-Model/data/patient_data_with_anpatel_" + str(datetime.date.today()) + ".p",
                         "wb"))



    print('completed')


#print(glob.glob("/home/software/Documents/ML-fax-Model/EAAM/2019-09-01_20_26_27_65286*.png"))
if __name__ == '__main__':
    #groupEMRData('MRI')
    #converttoText()
    #fetchPatientNameData()
    #GeneratePatInfoJSON()
    GeneratePatInfoJSON()
    # example_dict[i]['label']='79'
    # print(len(example_dict))
    # print(len(example_dict))
    #pickle.dump(example_dict, open("/home/software/Documents/ML-fax-Model/data/total_patient_data.p", "wb"))
