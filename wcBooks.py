import yaml
from tabulate import tabulate
from pyspark import SparkContext
from pyspark.sql import SQLContext

with open("books.yaml","r") as booksList:
    books = yaml.load(booksList)["information"]

books = map(lambda book: book["path"], books)

sc = SparkContext("local[4]", "BookWordCount")
sqlContext = SQLContext(sc)

wordList = sc.broadcast(["love", "war", "peace", "hate",
                         "man", "woman", "he", "she", "slave",
                         "freedom", "kill", "luck","him", "her"])
booksRDD = sc.textFile(','.join(books))

ranking = (booksRDD.flatMap(lambda line: line.split(" "))
                    .map(lambda word: word.lower())
                    .filter(lambda word: word in wordList.value)
                    .map(lambda word: (word, 1))
                    .reduceByKey(lambda a, b: a + b)
                    .collect())

ranking = dict(ranking)
sorted_rankings = sorted(ranking.items(), key=lambda (k,v): v, reverse=True)

print tabulate(sorted_rankings, headers=["Word", "Occurences"])