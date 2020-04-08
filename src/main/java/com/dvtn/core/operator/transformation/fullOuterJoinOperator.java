package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * fullOuterJoin:
 *  public <W> JavaPairRDD<K,scala.Tuple2<Optional<V>,Optional<W>>> fullOuterJoin(JavaPairRDD<K,W> other,int numPartitions)
 *  对两个RDD执行全连接，K为两个RDD出现的键，匹配到的在V,W位置显示相应的值，没有匹配到的显示None
 *
 *
 *
 */
public class fullOuterJoinOperator {
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


        //(一灯大师,(Optional[61],Optional.empty))
        //(东方不败,(Optional.empty,Optional[葵花宝典]))
        //(风清扬,(Optional[60],Optional[剑宗]))
        //(梅超风,(Optional[42],Optional.empty))
        //(独孤求败,(Optional.empty,Optional[剑圣]))
        //(张三丰,(Optional[100],Optional[太极拳]))

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoinRDD = rdd1.fullOuterJoin(rdd2);

        /**
         * 分区数的变化
         */
        System.out.println("rdd1和rdd2相fullOuterJoin后生成的RDD的分区数为："+fullOuterJoinRDD.partitions().size());

        fullOuterJoinRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, Optional<String>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Optional<Integer>, Optional<String>>> tuple) throws Exception {
                System.out.println(tuple);
            }
        });
    }
}
