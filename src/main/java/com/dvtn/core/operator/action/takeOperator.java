package com.dvtn.core.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * take Action类算子
 *  java.util.List<T> take(int num): 返回RDD中的前num个元素。如果要返回全部元素使用collect()；如果要返回第一个元素，使用first()
 *
 *  注意：
 *  1、first() = take(1)
 *  2、如果返回的结果太大有可能造成Driver端内存溢出问题
 */
public class takeOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("take");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> lines = sc.textFile("./data/words");

        //返回结果集的前10行
        List<String> take = lines.take(10);
        for(String s:take){
            System.out.println(s);
        }
    }
}
