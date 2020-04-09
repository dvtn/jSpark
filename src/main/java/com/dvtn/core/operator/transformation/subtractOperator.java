package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * subtract:取两个RDD的差集(可作用于KV,也可作用于非KV格式)
 * public JavaPairRDD<K,V> subtract(JavaPairRDD<K,V> other,int numPartitions)
 *
 */
public class subtractOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("intersection");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

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
         * rdd1与rdd2的差集是元素只在rdd1里有，在rdd2中没有
         */
        JavaPairRDD<String, String> subtractRDD = rdd1.subtract(rdd2);

        /**
         * subtract操作后生成的RDD的partition的个数是两个原生rdd分区个数较多的RDD
         *
         * subtract生成的RDD的分区个数为：4
         * (马蓉,王宝强)
         * (马蓉,王宝强)
         * (李小璐,贾乃亮)
         * (文章,马伊俐)
         */
        System.out.println("subtract生成的RDD的分区个数为："+subtractRDD.partitions().size());

        subtractRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tp) throws Exception {
                System.out.println(tp);
            }
        });

        //
        sc.stop();

    }
}
