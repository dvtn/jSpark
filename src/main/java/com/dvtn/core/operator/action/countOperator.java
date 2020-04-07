package com.dvtn.core.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * count Action类算子
 *      long count(): 返回RDD中元素的数量, 结果会被回收到Driver端
 */
public class countOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("count");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> lines = sc.textFile("./data/words");

        long elementsCount = lines.count();
        System.out.println("RDD中的元素个数为："+ elementsCount);
    }
}
