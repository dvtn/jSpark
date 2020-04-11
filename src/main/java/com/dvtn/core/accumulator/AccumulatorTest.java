package com.dvtn.core.accumulator;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

public class AccumulatorTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("AccumulatorTest");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        LongAccumulator longAccumulator = sc.sc().longAccumulator("MyLongAccumulator"); //"MyLongAccumulator"为该累加器在web UI上的名称

        JavaRDD<String> lines = sc.textFile("./data/words");

        JavaRDD<String> mapRDD = lines.map((line) -> {
            longAccumulator.add(1);
            return line;
        });

        mapRDD.collect();

        System.out.println("accumulator: "+longAccumulator.value());
    }
}
