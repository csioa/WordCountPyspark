import yaml
import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.style.use('ggplot')
import matplotlib.pyplot as plt
from tabulate import tabulate
from pyspark import SparkContext

FILENAME = "books.yaml"

KEY_WORDS = ["love", "war", "peace","freedom", "soul"
             "man", "woman","kill", "luck", "destiny"]


def plot_rankings(ranking):
    df = pd.DataFrame(ranking.values(), index=ranking.keys())

    plt.figure()

    ax = df.sort_values(0).plot.barh(legend=False)
    fig = ax.get_figure()
    fig.savefig('word_ranking.png')


def get_books(filename):
    with open(filename, "r") as booksList:
        books = yaml.load(booksList)["information"]

    books = map(lambda book: book["path"], books)
    return books


def spark_wordcount():
    sc = SparkContext("local[4]", "BookWordCount")

    wordList = sc.broadcast(KEY_WORDS)
    booksRDD = sc.textFile(','.join(get_books(FILENAME)))

    ranking = (booksRDD.flatMap(lambda line: line.split(" "))
               .map(lambda word: word.lower())
               .filter(lambda word: word in wordList.value)
               .map(lambda word: (word, 1))
               .reduceByKey(lambda a, b: a + b)
               .collect())
    return ranking


def main():

    ranking = spark_wordcount()

    ranking = dict(ranking)
    sorted_rankings = sorted(ranking.items(), key=lambda (k, v): v, reverse=True)

    print tabulate(sorted_rankings, headers=["Word", "Occurrences"])

    plot_rankings(ranking)

if __name__=="__main__":
    main()

