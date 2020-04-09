package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * distinct: 对RDD中的元素去重
 */
public class distinctOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("distinct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(
                "a", "a", "a", "a",
                "b", "b", "b", "b"
        ));

        /**
         * 传统方式实现RDD元素去重需要三步
         *  第一步：把RDD转换成K,V格式的RDD, K为元素，V为1
         *  每二步：对K,V格式的RDD的Key进行分组计算
         *  第三步：对得到的RDD只取第一位键
         */
        // [(a,1),(a,1),(a,1),(a,1),(b,1),b,1),b,1),b,1)]
        JavaPairRDD<String, Integer> mapToPairRDD = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        //对每个key进行聚合
        //[(a,4),(b,4)]
        JavaPairRDD<String, Integer> reduceRDD = mapToPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //只取键，不要值
        JavaRDD<String> mapRDD = reduceRDD.map(new Function<Tuple2<String, Integer>, String>() {

            @Override
            public String call(Tuple2<String, Integer> tuple) throws Exception {
                return tuple._1;
            }
        });

        mapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        System.out.println("-----------------------------------");


        //使用Spark提供的算子distinct实现RDD元素去重
        JavaRDD<String> distinctRDD = rdd1.distinct();

        distinctRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.stop();


    }
}
