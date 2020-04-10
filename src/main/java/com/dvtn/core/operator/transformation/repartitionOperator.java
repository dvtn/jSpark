package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * repartition:
 *  增加或减少分区。会产生shuffle。（多个分区分到一个分区不会产生shuffle）,可以对RDD重新分区
 *
 */
public class repartitionOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("mapPartitionWithIndex");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> parallelizeRDD = sc.parallelize(Arrays.asList(
                "风清扬11", "风清扬12", "风清扬13", "风清扬14",
                "任我行11", "任我行12", "任我行13", "任我行14",
                "黄药师11", "黄药师12", "黄药师13", "黄药师14"
        ),3);

        System.out.println("parallelizeRDD partition个数为："+parallelizeRDD.partitions().size());

        /**
         * mapPartitionsWithIndex
         * Function2中：
         * 第一个Integer参数代表partition的分区号
         * 每二个参数Iterator<String>代表每个分区中的元素
         * 第三个参数是返回的元素
         * preservePartitioning: 是否与父RDD保持相同的分区数
         *
         */
        JavaRDD<String> mapPartitionsWithIndexRDD = parallelizeRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<>();
                while (iter.hasNext()) {
                    String value = iter.next();
                    list.add("mapPartitionsWithIndexRDD partition index = 【" + index + "】, 【value = " + value + "】");
                }
                return list.iterator();
            }
        }, true);

        JavaRDD<String> repartitionRDD = mapPartitionsWithIndexRDD.repartition(4);
        JavaRDD<String> rdd2 = repartitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iter) throws Exception {
                List<String> list = new ArrayList<>();
                while (iter.hasNext()) {
                    String value = iter.next();
                    list.add("repartitionRDD partition index = 【" + index + "】, 【value = " + value + "】");
                }
                return list.iterator();
            }
        }, true);

        List<String> collect = rdd2.collect();
        for(String s:collect){
            System.out.println(s);
        }

        sc.stop();

    }
}
