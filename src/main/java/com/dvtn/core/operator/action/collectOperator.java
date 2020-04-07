package com.dvtn.core.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * collect:
 *  java.util.List<T> collect(): 返回以列表、数据形式返回RDD中所有元素,是Action算子。
 *
 *  注意：因为该action算子人把结果回收到Driver端的内存中，当结果太大时，会容易造成Driver端的OOM
 */
public class collectOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("collect");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> lines = sc.textFile("./data/words");

        //回收结果
        List<String> result = lines.collect();
        for(String s:result){
            System.out.println(s);
        }
    }
}
