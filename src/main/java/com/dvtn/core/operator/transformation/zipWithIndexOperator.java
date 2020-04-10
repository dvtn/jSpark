package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * zipWithIndex:
 * 给RDD中的每一个元素与当前元素的下标压缩成一个K,V格式的RDD
 */

public class zipWithIndexOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("zipWithIndex");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("风清扬", "剑"),
                new Tuple2<String, String>("风清扬", "剑宗"),
                new Tuple2<String, String>("风清扬", "独孤九剑"),
                new Tuple2<String, String>("张三丰", "太极"),
                new Tuple2<String, String>("张三丰", "太极拳"),
                new Tuple2<String, String>("张三丰", "太极真人"),
                new Tuple2<String, String>("东方不败", "宝典"),
                new Tuple2<String, String>("东方不败", "葵花宝典")
        ));

        /**
         * ((风清扬,剑),0)
         * ((风清扬,剑宗),1)
         * ((风清扬,独孤九剑),2)
         * ((张三丰,太极),3)
         * ((张三丰,太极拳),4)
         * ((张三丰,太极真人),5)
         * ((东方不败,宝典),6)
         * ((东方不败,葵花宝典),7)
         */
        JavaPairRDD<Tuple2<String, String>, Long> zipWithIndexRDD = rdd1.zipWithIndex();

        zipWithIndexRDD.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Long>>() {
            @Override
            public void call(Tuple2<Tuple2<String, String>, Long> tuple) throws Exception {
                System.out.println(tuple);
            }
        });

        sc.stop();
    }
}
