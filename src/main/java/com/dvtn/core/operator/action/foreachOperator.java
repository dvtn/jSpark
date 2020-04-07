package com.dvtn.core.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * foreach Action类算子
 *      void foreach(VoidFunction<T> f): 对RDD中的每个元素应用f函数的逻辑
 */
public class foreachOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("foreach");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> lines = sc.textFile("./data/words");

        lines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                if(s.startsWith("金庸")) {
                    System.out.println(s);
                }
            }
        });
    }
}
