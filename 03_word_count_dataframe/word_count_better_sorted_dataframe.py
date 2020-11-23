import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("WordCount").getOrCreate()


def main():
    # Read each line of data into a dataframe
    inputDF = spark.read.text("book.txt")

    # split using a regex
    words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
    words.filter(words.word != "")

    # to lowercase
    lowercase_words = words.select(func.lower(words.word).alias("word"))

    # count each word
    word_counts = lowercase_words.groupBy("word").count()

    # sort by counts
    word_counts_sorted = word_counts.sort("count")

    # show results
    word_counts_sorted.show(word_counts_sorted.count())


if __name__ == "__main__":
    main()