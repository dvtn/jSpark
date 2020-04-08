package com.dvtn.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * parallelizePairs: 根据集合生成KV格式的RDD
 *  public <K,V> JavaPairRDD<K,V> parallelizePairs(java.util.List<scala.Tuple2<K,V>> list,int numSlices)
 *      参数说明：
 *          list: 其元素为List<scala.Tuple2<K,V>>格式的元素
 *          numSlices: 可选参数，用来指定生成的RDD的分区个数，默认未指定分区数为1.
 *
 */
public class parallelizePairsCreateRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("createRDDwitParallelizePairs");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("WARN");

        List<Tuple2<String, Integer>> nameList = Arrays.asList(
                new Tuple2<String, Integer>("风清扬", 60),
                new Tuple2<String, Integer>("张三丰", 100),
                new Tuple2<String, Integer>("独孤求败", 40),
                new Tuple2<String, Integer>("东方不败", 38),
                new Tuple2<String, Integer>("任我行", 58),
                new Tuple2<String, Integer>("王重阳", 62),
                new Tuple2<String, Integer>("洪七公", 55),
                new Tuple2<String, Integer>("老顽童", 50),
                new Tuple2<String, Integer>("黄药师", 56),
                new Tuple2<String, Integer>("欧阳锋", 57),
                new Tuple2<String, Integer>("一灯大师", 61),
                new Tuple2<String, Integer>("梅超风", 42)
        );

        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(nameList);
        System.out.println("parallelizePairs生成的RDD的默认分区个数为："+rdd1.partitions().size());
        rdd1.collect();

        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(nameList, 10);
        System.out.println("parallelizePairs指定numSlices为10后生成的RDD的分区个数为："+rdd2.partitions().size());
        rdd2.collect();

        sc.stop();

    }
}
