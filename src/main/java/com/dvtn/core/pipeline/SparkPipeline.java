package com.dvtn.core.pipeline;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class SparkPipeline {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("SparkPipelineTest");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        JavaRDD<Integer> rdd2 = rdd1.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i) throws Exception {
                System.out.println("map-------------------->" + i);
                return i;
            }
        });

        JavaRDD<Integer> rdd3 = rdd2.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {

                System.out.println("filter-----------------" + integer);
                return true;
            }
        });

        rdd3.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("foreach-------------------"+integer);
            }
        });

        sc.stop();


    }
}
