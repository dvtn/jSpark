package com.dvtn.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * textFile: 通过本地或Hadoop文件生成RDD
 * public JavaRDD<String> textFile(String path, int minPartitions)
 *  参数说明：
 *      path: 文件的路径
 *      minPartitions: 生成的最小分区个数，可选参数, 未指定时生成的RDD的默认分区个数为1.
 *
  */
public class textFileCreateRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("count");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> rdd1 = sc.textFile("./data/words");
        System.out.println("生成的RDD的默认分区个数为："+rdd1.partitions().size());
        rdd1.collect();

        JavaRDD<String> rdd2 = sc.textFile("./data/words", 100);
        System.out.println("生成的RDD的指定分区个数为："+rdd2.partitions().size());
        rdd2.collect();


        sc.stop();
    }
}
