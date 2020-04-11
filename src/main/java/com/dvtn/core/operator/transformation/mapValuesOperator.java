package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * public <U> JavaPairRDD<K,U> mapValues(Function<V,U> f)
 * Pass each value in the key-value pair RDD through a map function without changing the keys;
 * this also retains the original RDD's partitioning.
 *
 *  只关心values
 * 针对于(K,V)形式的类型只对V进行操作
 *
 */
public class mapValuesOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("mapValues");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaPairRDD<String, Integer> parallelizePairsRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("风清扬", 100),
                new Tuple2<String, Integer>("东方不败", 200),
                new Tuple2<String, Integer>("任我行", 300)
        ));

        /**
         * mapValues只针对K,V格式RDD中的value进行操作
         */
        JavaPairRDD<String, Integer> mapValuesRDD = parallelizePairsRDD.mapValues(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i) throws Exception {
                return i + 25;
            }
        });

        mapValuesRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple);
            }
        });

        sc.stop();
    }
}
