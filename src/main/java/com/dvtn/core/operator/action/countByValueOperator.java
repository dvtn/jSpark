package com.dvtn.core.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * countByValue: Action类算子，用于对RDD中相同的元素计数
 *
 * java.util.Map<T,Long> countByValue()
 * Return the count of each unique value in this RDD as a map of (value, count) pairs.
 * The final combine step happens locally on the master, equivalent to running a single reduce task.
 *
 */
public class countByValueOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("countByValue");

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
         * key = (东方不败,300), value = 1
         * key = (东方不败,30), value = 1
         * key = (张三丰,20), value = 1
         * key = (风清扬,1000), value = 1
         * key = (张三丰,200), value = 1
         * key = (风清扬,100), value = 2
         * key = (张三丰,2000), value = 1
         */

        Map<Tuple2<String, Integer>, Long> countByValueResult = rdd.countByValue();
        Set<Map.Entry<Tuple2<String, Integer>, Long>> entries = countByValueResult.entrySet();
        for(Map.Entry<Tuple2<String, Integer>, Long> entry:entries){
            Tuple2<String, Integer> key = entry.getKey();
            Long value = entry.getValue();

            System.out.println("key = "+ key + ", value = " + value);
        }

        sc.stop();
    }
}
