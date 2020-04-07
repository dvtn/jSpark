package com.dvtn.core.operator.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * persist: 懒执行算子，需要action算子触发执行。
 *      public JavaRDD<T> persist(StorageLevel newLevel)
 *      可以手动指定持久化的级别。
 *
 * StorageLevel:
 *
 *    public StorageLevel(final boolean _useDisk, final boolean _useMemory, final boolean _useOffHeap, final boolean _deserialized, final int _replication) {
 *       this._useDisk = _useDisk;
 *       this._useMemory = _useMemory;
 *       this._useOffHeap = _useOffHeap;
 *       this._deserialized = _deserialized;
 *       this._replication = _replication;
 *
 *
 *    private final StorageLevel NONE;
 *    private final StorageLevel DISK_ONLY;
 *    private final StorageLevel DISK_ONLY_2;
 *    private final StorageLevel MEMORY_ONLY;    (常用)
 *    private final StorageLevel MEMORY_ONLY_2;
 *    private final StorageLevel MEMORY_ONLY_SER;    (常用)
 *    private final StorageLevel MEMORY_ONLY_SER_2;
 *    private final StorageLevel MEMORY_AND_DISK;    (常用) 内存不够了再放磁盘
 *    private final StorageLevel MEMORY_AND_DISK_2;
 *    private final StorageLevel MEMORY_AND_DISK_SER;  (常用)
 *    private final StorageLevel MEMORY_AND_DISK_SER_2;
 *    private final StorageLevel OFF_HEAP;
 *
 *  注意：cache和persist
 *  1.cache和persist都是懒执行，需要Action类算子触发执行。
 *  2.对一个RDD cache或者persist之后可以赋值给一个变量，下次直接使用这个变量就是相当于使用持久化的RDD。
 *  3.如果赋值给一个变量，那么cache和persist后不能紧跟Action算子。
 */
public class persistOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("cache");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> rdd = sc.textFile("./data/orders.csv"); //orders.csv 109M

        /**
         * cache() = persist() = persist(StorageLevel.MEMORY_ONLY)
         */
        //rdd.persist(StorageLevel.MEMORY_ONLY());

        JavaRDD<String> rdd1 = rdd.persist(StorageLevel.MEMORY_ONLY());

        //如果赋值给一个变量，那么cache和persist后不能紧跟Action算子。
        //JavaRDD<String> rdd1 = rdd.persist(StorageLevel.MEMORY_ONLY()).collect();

        long startTime1 = System.currentTimeMillis();
        long result1 = rdd1.count();
        long endTime1 = System.currentTimeMillis();

        System.out.println("result1: "+result1 + ", 耗时："+(endTime1-startTime1)+" 毫秒"); //来源一磁盘

        long startTime2 = System.currentTimeMillis();
        long result2 = rdd1.count();
        long endTime2 = System.currentTimeMillis();
        System.out.println("result2: "+result2 + ", 耗时："+(endTime2-startTime2)+" 毫秒"); //来源于cache后的内存中

        sc.stop();
    }
}
