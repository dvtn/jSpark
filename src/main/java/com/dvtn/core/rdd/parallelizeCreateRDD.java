package com.dvtn.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.List;

/**
 * parallelize: 根据集合生成RDD
 *  public <T> JavaRDD<T> parallelize(java.util.List<T> list,int numSlices)
 *      参数说明：
 *          list: List集合对象
 *          numSlices: 生成指定分区数的RDD,该参数是可选参数,未指定生成的RDD的默认分区个数为1
 *
 */
public class parallelizeCreateRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("createRDDwithParallelize");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("WARN");


        List<String> nameList = Arrays.asList(
                "风清扬", "张三丰", "独孤求败", "东方不败",
                "任我行", "王重阳", "洪七公", "老顽童",
                "黄药师", "欧阳锋", "一灯大师", "梅超风");

        JavaRDD<String> rdd1 = sc.parallelize(nameList);
        System.out.println("parallelize生成的RDD的默认分区个数为："+rdd1.partitions().size());
        rdd1.collect();

        JavaRDD<String> rdd2 = sc.parallelize(nameList, 10);
        System.out.println("parallelize指定numSlices为10后生成的RDD的分区个数为："+rdd2.partitions().size());
        rdd2.collect();

        sc.stop();
    }
}
