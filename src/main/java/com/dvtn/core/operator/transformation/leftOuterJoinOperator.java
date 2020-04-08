package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * leftOuterJoin: 左外连接
 *  public <W> JavaPairRDD<K,scala.Tuple2<V,Optional<W>>> leftOuterJoin(JavaPairRDD<K,W> other,int numPartitions)
 *      对this和other两个RDD执行左外连接，以this为主，结果包含this中所有的键和值以及other中匹配的键值 ==> (K, (V, W))
 *      对于this中有的键，other中没有的，W位置为None  ===> (K, (V, None))
 *
 *
 */
public class leftOuterJoinOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("leftOuterJoin");

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
         * 第一个String为rdd1中所有Key;
         * Integer为rdd1中所有键对应的值;
         * Optional<String>为rdd2中通过Key关联后的值，有，则为Optional[值], 没有则为Optional.empty;
         */

        /**
         * (一灯大师,(61,Optional.empty))
         * (风清扬,(60,Optional[剑宗]))
         * (梅超风,(42,Optional.empty))
         * (张三丰,(100,Optional[太极拳]))
         */

        JavaPairRDD<String, Tuple2<Integer, Optional<String>>> leftOuterJoinRDD = rdd1.leftOuterJoin(rdd2);

        /**
         * 分区数的变化
         */
        System.out.println("rdd1和rdd2相leftOuterJoin后生成的RDD的分区数为："+leftOuterJoinRDD.partitions().size());

        leftOuterJoinRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<String>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Optional<String>>> tuple) throws Exception {

                String key = tuple._1;
                Integer v1 = tuple._2._1;
                Optional<String> v2 = tuple._2._2;
                //获取Optional中的值
                if(v2.isPresent()){
                    System.out.println(key+"---"+v1+"---"+v2.get());
                }else {
                    System.out.println(key+"---"+v1+"---"+"null");
                }
            }
        });

        sc.stop();
    }
}
