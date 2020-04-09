package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * intersection: 可以作用于KV格式的RDD, 也可以作用于非KV格式的RDD
 *  public JavaPairRDD<K,V> intersection(JavaPairRDD<K,V> other)
 *
 *      返回两个K,V格式的RDD的交集，同时实现去重功能
 *
 * 注意：
 * 1.即使原来某个RDD中有重复的元素，在intersection后，会只保留一份，实现去重功能；
 * 2.intersection后的RDD的分区数是两个RDD的分区数多的保持一致
 */

public class intersectionOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("intersection");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");//设置log级别

        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("文章", "马伊俐"),
                new Tuple2<String, String>("李小璐","贾乃亮"),
                new Tuple2<String, String>("马蓉", "王宝强"),
                new Tuple2<String, String>("马蓉", "王宝强"),
                new Tuple2<String, String>("羽凡", "白百何"),
                new Tuple2<String, String>("刘恺威", "杨幂")
        ), 4);

        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("文章", "姚笛"),
                new Tuple2<String, String>("李小璐", "PG"),
                new Tuple2<String, String>("马蓉", "宋喆"),
                new Tuple2<String, String>("羽凡", "白百何"),
                new Tuple2<String, String>("刘恺威", "杨幂")
        ), 2);

        /**
         * intersection后RDD的分区数为：4
         * (刘恺威,杨幂)
         * (羽凡,白百何)
         *
         */

        JavaPairRDD<String, String> intersectionRDD = rdd1.intersection(rdd2);

        System.out.println("intersection后RDD的分区数为："+intersectionRDD.partitions().size());

        intersectionRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tuple) throws Exception {
                System.out.println(tuple);
            }
        });

    }
}
