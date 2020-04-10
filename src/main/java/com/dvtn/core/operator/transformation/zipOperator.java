package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * zip:
 * 将两个RDD压缩成一个K,V格式的RDD
 * 两个RDD中每个分区数和每个分区的元素数量要一致
 */

public class zipOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("groupByKey");

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

        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("风清扬", 10),
                new Tuple2<String, Integer>("风清扬", 100),
                new Tuple2<String, Integer>("风清扬", 1000),
                new Tuple2<String, Integer>("张三丰", 20),
                new Tuple2<String, Integer>("张三丰", 200),
                new Tuple2<String, Integer>("张三丰", 2000),
                new Tuple2<String, Integer>("东方不败", 30),
                new Tuple2<String, Integer>("东方不败", 300)
        ));

        JavaPairRDD<Tuple2<String, String>, Tuple2<String, Integer>> zipRDD = rdd1.zip(rdd2);

        /**
         * ((风清扬,剑),(风清扬,10))
         * ((风清扬,剑宗),(风清扬,100))
         * ((风清扬,独孤九剑),(风清扬,1000))
         * ((张三丰,太极),(张三丰,20))
         * ((张三丰,太极拳),(张三丰,200))
         * ((张三丰,太极真人),(张三丰,2000))
         * ((东方不败,宝典),(东方不败,30))
         * ((东方不败,葵花宝典),(东方不败,300))
         */
        zipRDD.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Tuple2<String, String>, Tuple2<String, Integer>> tuple) throws Exception {
                System.out.println(tuple);
            }
        });

        sc.stop();

    }
}
