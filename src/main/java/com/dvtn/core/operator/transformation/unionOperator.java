package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * union: 合并RDD
 *  public JavaPairRDD<K,V> union(JavaPairRDD<K,V> other)
 *      合并两个K,V格式的RDD,两个RDD的类型必须相同，结果包含重复项，如果要去重，使用distinct()算子转化。
 *
 *  注意：
 *  1.两个RDD类型须保持一致
 *  2.生成的新的RDD的分区数为两个RDD分区的和
 */
public class unionOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("union");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("文章", "马伊俐"),
                new Tuple2<String, String>("李小璐","贾乃亮"),
                new Tuple2<String, String>("马蓉", "王宝强"),
                new Tuple2<String, String>("刘恺威", "杨幂")
        ), 4);

        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("文章", "姚笛"),
                new Tuple2<String, String>("李小璐", "PG"),
                new Tuple2<String, String>("马蓉", "宋喆"),
                new Tuple2<String, String>("刘恺威", "杨幂")
        ), 2);


        JavaPairRDD<String, String> unionRDD = rdd1.union(rdd2);

        System.out.println("union后RDD的分区个数为："+unionRDD.partitions().size());

        unionRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tuple) throws Exception {
                System.out.println(tuple);
            }
        });
    }
}
