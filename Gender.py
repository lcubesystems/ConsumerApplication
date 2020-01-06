import numpy as np
import pickle

class GenderClassifier:

    def gender_features(self, name):
        name = name.lower()
        features = {}
        features["last_letter"] = name[-1]
        features["first_letter"] = name[0]
        features["prefix2"] = name[0:2]
        features["prefix3"] = name[0:3]
        features["suffix2"] = name[-2:]
        features["suffix3"] = name[-3:]
        features["suffix4"] = name[-4:]
        return features

    def predict(self, name):
        print('name '+name)
        lr_model = pickle.load(open("gender_classifier.mod", "rb"))
        gender = lr_model.predict(self.gender_features(name))
        print('name'+str(gender[0]))
        return gender[0]
