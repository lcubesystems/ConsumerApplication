import re
import datefinder

import  Logging
import logging

Logging.configure_logging('/tmp/kafkalog')

def extract_patient_info(file_content,classname,patient_name_extraction_model):

    #print(file_content)


    # TODO:
    #  Need to initialize this object during application init
    #  Need to change the model path. (We should hard code this or get it from a property file.)


    if(classname=='Prior Authorization'):
        name_regex = ["(?i)PA for\s(:|\s|)+","(?i)patient name(:|\s|)+", "Pt(:|\s|)+", "(?i)(\b|^)Patient(:|\s)+", "(?i)(\b|^)Name(:|\s|)+",
                  "(?i)resident(:|\s|)+", "(^)(RE|Re)(:|\s)+"]
    elif ('cvs/pharmacy' in str(file_content)):
        print('Matched cvs pharmacy')
        name_regex = ["(?i)(\b|^)Patient(:|\s)+(?i)(\b|^)Name(:|\s|)+","(?i)(\s|^)Patient(:|\s)+(?i)(\s|^)Name(:|\s|)+", "(?i)(\b|^)Patient(:|\s)+"]
        file_content = str(file_content).replace('\n\n', ' ')
    else:
        name_regex = ["(?i)patient name(:|\s|)+", "Pt(:|\s|)+", "(?i)(\b|^)Patient(:|\s)+","(?i)(\s|^)Patient(:|\s)+", "(?i)(\b|^)Name(:|\s|)+","(?i)(\s|^)Name(:|\s|)+",
                      "(?i)resident(:|\s|)+", "(^)(RE|Re)(:|\s)+"]
    mr_regex = ["(\b|\()MR\s(#|No)(:|\s)+"]
    dob_regex = ["(?i)Date\sof\sBirth(:|\s|)+", "(?i)DOB(:|\s|)+", "(?i)Birth Date(:|\s|)+", "      "]
    names = []
    dob = ""

    lines = file_content.splitlines()


    name_found = False
    dob_found = False

    for j in range(len(lines)):
        if not name_found:
            for p in range(len(name_regex)):
                if lines[j].strip() and 'patient details' not in lines[j].strip().lower():
                    match = re.search(name_regex[p], lines[j])
                    #print('match : '+str(str(match) + ': ' +lines[j] ))
                    if match:
                        start = match.start()
                        end = match.end()

                        names, dob = get_patient_info(patient_name_extraction_model, lines[j][start:].strip())

                        matches = datefinder.find_dates(lines[j][start:].strip())
                        logging.info(lines[j][start:].strip())
                        for match in matches:
                            logging.info('DOB Matching-- > '+lines[j][start:].strip()+' : \n '+str(match))

                        if len(names) == 0:
                            if len(lines) > j+1:
                                names, dob = get_patient_info(patient_name_extraction_model, lines[j+1].strip())

                        if len(names) >= 1:
                            name_found = True
                            break
                        if dob:
                            dob_found = True
                    else:
                        continue

        if not name_found:
            for p in range(len(mr_regex)):
                match = re.search(mr_regex[p], lines[j])
                if match:
                    start = match.start()
                    names, dob = get_patient_info(patient_name_extraction_model, lines[j][0:start+1].strip())
                    if len(names) == 0:
                        if len(lines) > j+1:
                            names, dob = get_patient_info(patient_name_extraction_model, lines[j+1].strip())

                    if len(names) >= 1:
                        name_found = True
                        break
                    if dob:
                        dob_found = True
                else:
                    continue

        if name_found and not dob_found:
            for p in range(len(dob_regex)):
                match = re.search(dob_regex[p], lines[j])
                matches = datefinder.find_dates(lines[j].strip())
                logging.info(lines[j].strip())
                for matchDOB in matches:
                    logging.info('DOB Matching-- > ' + lines[j].strip() + ' : \n ' + str(matchDOB))
                if match:
                    end = match.end()
                    dob = get_dob_info(patient_name_extraction_model, lines[j][end:].strip())

                if dob:
                    dob_found = True
                    break

        if name_found and dob_found:
            break

    patient_info = dict()
    patient_info["names"] = names
    patient_info["dob"] = dob
    return patient_info





def get_patient_info(model, line):
    nlp = model(line)
    names = []
    dob = ""
    for e in nlp.ents:
        if e.label_ == "B-PATIENT" or e.label_ == "I-PATIENT" or e.label_ == "I-PROVIDER":
            names.append(str(e.text).replace('\'',''))

        elif e.label_ == "B-DOB":
            dob = e.text
    if len(names) < 2:
        names = []
    return names, dob


def get_dob_info(model, line):
    nlp = model(line)
    dob = ""
    for e in nlp.ents:
        print(e.label_, e.text)
        if e.label_ == "B-DOB":
            dob = e.text
    if not dob:
        dob = get_dob_by_regex(line)
    return dob


def get_dob_by_regex(line):
    match = re.search("(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May?|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?"
                      "|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}", line)
    if match:
        return line[match.start():match.end()]
    else:
        return ""

