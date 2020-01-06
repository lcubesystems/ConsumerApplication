import torch
import string
from DLModels import RNN

class EthnicityClassifier:

    def line_to_tensor(self, person_name):
        all_letters = string.ascii_letters + " .,;'"
        n_letters = len(all_letters)
        tensor = torch.zeros(len(person_name), 1, n_letters)
        for li, letter in enumerate(person_name):
            tensor[li][0][all_letters.find(letter)] = 1
        return tensor

    def predict(self, person_name):
        try:
            model = torch.load("ethnicity_classifier.mod")
            hidden = model.initHidden()
            line_tensor = self.line_to_tensor(person_name.upper())

            for i in range(line_tensor.size()[0]):
                output, hidden = model(line_tensor[i], hidden)

            sm = torch.nn.Softmax(dim=1)

            probabilities = sm(output).detach().numpy()[0]


            prediction = 0
            print('pro-->'+str(probabilities))
            if probabilities[1] > 0.7:
                print('success'+str(probabilities[1]))
                prediction = 1
        except (Exception) as error:
            print(error)
        print(str(probabilities))
        return prediction, probabilities
