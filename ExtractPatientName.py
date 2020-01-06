import nltk
nltk.download('maxent_ne_chunker')
nltk.download('words')
import pickle
import requests
import spacy
pickle_in = open("/home/software/Documents/ML-fax-Model/data/name_extraction_2019-12-10.p","rb")
example_dict = pickle.load(pickle_in)
for i in example_dict:

    # function to test if something is a noun
    is_noun = lambda pos: pos[:2] == 'NN'
    # do the nlp stuff
    print(i['patient_name'])
    pateint_name =str(i['patient_name']).split(', ')
    tokenized = nltk.word_tokenize(i['content'])
    nouns = [word for (word, pos) in nltk.pos_tag(tokenized) if is_noun(pos)]
    #print(pateint_name)
    print(set(pateint_name).intersection(nouns))


    #print( pateint_name in nouns)

print( nouns)
#new method
import nltk

def extract_entities(text):
   print('enteres')
   for sent in nltk.sent_tokenize(text):

       for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent))):
         if hasattr(chunk, 'Summer'):
            print (chunk.node, ' '.join(c[0] for c in chunk.leaves()))

extract_entities('Summer School of the Arts filling fast\nWanganui people have the chance to learn the intricacies of')


# new method
import nltk
from nltk.corpus import wordnet
nltk.download('wordnet')
person_names='Person'
person_list = []
def get_human_names(text):
    tokens = nltk.tokenize.word_tokenize(text)
    pos = nltk.pos_tag(tokens)
    sentt = nltk.ne_chunk(pos, binary = False)

    person = []
    name = ""
    for subtree in sentt.subtrees(filter=lambda t: t.label() == 'PERSON'):
        for leaf in subtree.leaves():
            person.append(leaf[0])
        if len(person) > 1: #avoid grabbing lone surnames
            for part in person:
                name += part + ' '
            if name[:-1] not in person_list:
                person_list.append(name[:-1])
            name = ''
        person = []
#     print (person_list)



names = get_human_names(example_dict[1]['content'])
for person in person_list:
    person_split = person.split(" ")
    for name in person_split:
        if wordnet.synsets(name):
            if(name in person):
                person_names.remove(person)
                break

print(person_names)