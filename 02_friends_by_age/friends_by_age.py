import findspark
findspark.init()

from pyspark import SparkConf, SparkContext


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


def main():
    conf = SparkConf().setMaster('local').setAppName('FriendByAge')
    sc = SparkContext(conf=conf)
    lines = sc.textFile('fakefriends.csv')
    rdd = lines.map(parseLine)
    total_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    average_by_age = total_by_age.mapValues(lambda x: x[0] / x[1])
    results = average_by_age.collect()
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
