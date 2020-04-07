package com.dvtn.core.operator.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * cache: 懒执行的持久化算子
 *      public JavaRDD<T> cache(): 将RDD持久化到内存(Storage Level: MEMORY_ONLY)
 *
 * cache() = persist() = persist(StorageLevel.MEMORY_ONLY)
 *
 */
public class cacheOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("cache");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> rdd = sc.textFile("./data/orders.csv"); //orders.csv 109M

        //将rdd持久化到内存中
        rdd.cache(); //第一次cache()为空

        long startTime1 = System.currentTimeMillis();
        long result1 = rdd.count();
        long endTime1 = System.currentTimeMillis();

        System.out.println("result1: "+result1 + ", 耗时："+(endTime1-startTime1)+" 毫秒"); //来源一磁盘

        long startTime2 = System.currentTimeMillis();
        long result2 = rdd.count();
        long endTime2 = System.currentTimeMillis();
        System.out.println("result2: "+result2 + ", 耗时："+(endTime2-startTime2)+" 毫秒"); //来源于cache后的内存中

        sc.close();
    }
}
