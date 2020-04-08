package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * rightOuterJoin:
 *  public <W> JavaPairRDD<K,scala.Tuple2<Optional<V>,W>> rightOuterJoin(JavaPairRDD<K,W> other,int numPartitions)
 *      对this和other两个RDD执行右外连接，以other为主，结果包含other中所有的键和值以及other中匹配的键值 ==> (K, (V, W))
 *      对于other中有的键，this中没有的，V位置为None  ===> (K, (None, W))
 *
 */
public class rightOuterJoinOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("rightOuterJoin");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("风清扬", 60),
                new Tuple2<String, Integer>("张三丰", 100),
                new Tuple2<String, Integer>("一灯大师", 61),
                new Tuple2<String, Integer>("梅超风", 42)
        ), 4);

        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("风清扬", "剑宗"),
                new Tuple2<String, String>("张三丰", "太极拳"),
                new Tuple2<String, String>("独孤求败", "剑圣"),
                new Tuple2<String, String>("东方不败", "葵花宝典")
        ), 2);

        /**
         * 第一个String为rdd2中所有Key;
         * Optional<Integer>为与rdd2中所有键匹配的值，有，则为Optional[值],如果没有对应Key, 返回Optional.empty;
         * String为rdd2中所有Key对应的值
         */

        /**
         (东方不败,(Optional.empty,葵花宝典))
         (风清扬,(Optional[60],剑宗))
         (独孤求败,(Optional.empty,剑圣))
         (张三丰,(Optional[100],太极拳))
         */

        JavaPairRDD<String, Tuple2<Optional<Integer>, String>> rightOuterJoinRDD = rdd1.rightOuterJoin(rdd2);

        /**
         * 分区数的变化
         */
        System.out.println("rdd1和rdd2相rightOuterJoin后生成的RDD的分区数为："+rightOuterJoinRDD.partitions().size());

        rightOuterJoinRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, String>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Optional<Integer>, String>> tuple) throws Exception {
                System.out.println(tuple);
            }
        });

        sc.stop();
    }
}
