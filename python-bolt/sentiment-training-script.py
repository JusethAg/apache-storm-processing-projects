import pandas as pd
import scipy
import numpy
import sklearn

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import BernoulliNB
from sklearn.externals import joblib

dataFile = './sentiment-labelled-data/imdb_labelled.txt'
lines = data = pd.read_csv(dataFile, sep='\t', header = None, names = ['sentence', 'label'])

count_vectorizer = CountVectorizer(binary = 'true')
train_documents = count_vectorizer.fit_transform(lines['sentence'])

classifier = BernoulliNB.fit(train_documents, lines['label'])
classifier.predict(count_vectorizer.transform(['this is the worst movie']))

joblib.dump(classifier, 'SAModel.pkl')
joblib.dump(count_vectorizer, 'Vectorizer.pkl')