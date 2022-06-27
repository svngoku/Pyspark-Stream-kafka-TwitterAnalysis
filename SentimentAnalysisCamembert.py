import tensorflow as tf
from transformers import AutoTokenizer, TFAutoModelForSequenceClassification
from transformers import pipeline

tokenizer = AutoTokenizer.from_pretrained("tblard/tf-allocine")
model = TFAutoModelForSequenceClassification.from_pretrained("tblard/tf-allocine")

nlp = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)


print(nlp("Alad'2 est clairement le meilleur film de l'année 2018.")) # POSITIVE
print(nlp("Juste whoaaahouuu !")) # POSITIVE
print(nlp("NUL...A...CHIER ! FIN DE TRANSMISSION.")) # NEGATIVE
print(nlp("Je m'attendais à mieux de la part de Franck Dubosc !")) # NEGATIVE