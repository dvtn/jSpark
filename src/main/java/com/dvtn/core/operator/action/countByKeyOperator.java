package com.dvtn.core.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * countByKey:
 *  public java.util.Map<K,Long> countByKey()
 *      Count the number of elements for each key, and return the result to the master as a Map.
 *      统计每个键出现的数量，以Map返回,是一个action类算子。
 */
public class countByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("countByKey");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("风清扬", 100),
                new Tuple2<String, Integer>("风清扬", 100),
                new Tuple2<String, Integer>("风清扬", 1000),
                new Tuple2<String, Integer>("张三丰", 20),
                new Tuple2<String, Integer>("张三丰", 200),
                new Tuple2<String, Integer>("张三丰", 2000),
                new Tuple2<String, Integer>("东方不败", 30),
                new Tuple2<String, Integer>("东方不败", 300)
        ));

        /**
         * 东方不败---2
         * 张三丰---3
         * 风清扬---3
         */
        Map<String, Long> mapResult = rdd.countByKey();
        Set<Map.Entry<String, Long>> entries = mapResult.entrySet();
        for(Map.Entry<String, Long> entry:entries){
            System.out.println(entry.getKey()+"---"+entry.getValue());
        }

        sc.stop();
    }
}
