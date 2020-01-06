"""
    Document classification and utilities
"""

import pickle
import pandas as pd
from os import path


class DocumentClassifier:

    def predict_category(self, file_content,accoutId):
        """
        :param file_content: array of document content. Each entry in this array is a page content.
        :return: dict, containing the following info
                    predicted category of the document
                        id
                        label
                        confidence
                    is first page a cover page
                    list of confidence for all the available document categories
        """

        result = dict()
        model_input = pd.DataFrame(columns=["document"])

        # TODO:
        #  Need to initialize this object during application init
        #  Need to change the model path. (We should hard code this or get it from a property file.)

        if path.exists('models/fax_classification_model_' + accoutId + '.mod'):
            document_category_prediction_model = pickle.load(open("models/fax_classification_model_"+accoutId+".mod", "rb"))
        else:
            document_category_prediction_model = pickle.load(open("models/fax_classification_model.mod", "rb"))

        cover_page_model = CoverPagePredictor()

        is_cover_page = cover_page_model.check_cover_page(file_content[0])
        result["is_cover_page"] = is_cover_page
        print('type')
        print(type(file_content))
        if is_cover_page == 0:
            full_content = ''.join(file_content)
        else:
            if len(file_content) == 1:
                full_content = ''.join(file_content)
            else:
                full_content = ''.join(file_content[1:len(file_content)])
        print(type(full_content))
        model_input = model_input.append({"document": full_content}, ignore_index=True)

        labels = self.get_labels()

        predicted_prob = document_category_prediction_model.predict_proba(model_input["document"])[0]
        predicted_class = document_category_prediction_model.predict(model_input["document"])[0]

        result["predicted_class"] = predicted_class
        result["predicted_class_label"] = labels[predicted_class]
        result["predicted_class_name"] = labels[predicted_class]

        probabilities = []

        for i in range(len(predicted_prob)):
            probability = dict()
            probability["class"] = document_category_prediction_model.classes_[i]

            if document_category_prediction_model.classes_[i] == predicted_class:
                result["predicted_class_confidence"] = predicted_prob[i]
            print(str(document_category_prediction_model.classes_[i]))
            probability["class_label"] = labels[document_category_prediction_model.classes_[i]]
            probability["confidence"] = predicted_prob[i]
            probabilities.append(probability)

        result["classes"] = probabilities

        return result

    def get_labels(self):
        """
        This will return the list of categories and ids

        :return: dict, containing id as key and label as value
        """
        labels_list = pd.read_csv(open("labels.csv", "r"))
        labels = dict()
        for i in range(len(labels_list)):
            key = labels_list.iloc[i]["index"]
            value = labels_list.iloc[i]["name"]
            labels[key] = value
        return labels


class CoverPagePredictor:

    def check_cover_page(self, file_content=""):
        """
        Function to check whether page is a cover page or not

        :param file_content:
        :return: 0 - Not a cover page   1 - Cover page
        """

        document_input = pd.DataFrame(columns=["document"])
        document_input = document_input.append({"document": file_content}, ignore_index=True)

        # TODO:
        #  Need to initialize this object during application init
        #  Need to change the model path. (We should hard code this or get it from a property file.)
        model = pickle.load(open("cover_page_model.mod", "rb"))

        is_cover_page = model.predict(document_input["document"])[0]
        return is_cover_page