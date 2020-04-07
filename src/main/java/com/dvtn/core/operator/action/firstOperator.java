package com.dvtn.core.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * first:
 * T first(): 返回RDD中的第一个元素
 *
 * first() = take(1)
 */
public class firstOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("first");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> lines = sc.textFile("./data/words");

        String first = lines.first();
        System.out.println("first: "+first);

    }
}
