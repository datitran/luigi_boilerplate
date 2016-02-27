from pyspark import SparkContext
import numpy as np

def create_sc(spark):
    sc = spark
    return sc

def main():
    sc_spark = create_sc(SparkContext())
    test = sc_spark.parallelize(list(range(0,100)))
    total = test.sum()
    sc_spark.stop()
    return total

if __name__ == "__main__":
    total = main()
    with open("target/task2_dummy.csv", "w") as f:
        f.write(str(total))

