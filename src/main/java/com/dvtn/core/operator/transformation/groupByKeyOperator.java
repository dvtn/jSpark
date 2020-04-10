package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * groupByKey:
 * 作用在K，V格式的RDD上。根据Key进行分组。作用在（K，V），返回（K，Iterable <V>）
 */

public class groupByKeyOperator {
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

        //(东方不败,[宝典, 葵花宝典])
        //(张三丰,[太极, 太极拳, 太极真人])
        //(风清扬,[剑, 剑宗, 独孤九剑])
        JavaPairRDD<String, Iterable<String>> groupByKeyRDD = rdd1.groupByKey();
        groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                System.out.println(tuple);
            }
        });


        sc.stop();
    }
}
