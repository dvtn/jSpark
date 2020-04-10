package com.dvtn.core.pvuv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class WebsiteUv {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local").setAppName("WebsiteUvApp");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        /**
         * 相同的IP+相同的网址作为Key
         */

        //读取数据
        JavaRDD<String> lines = sc.textFile("./data/pvuvdata");


        JavaRDD<String> IpWebsiteRdd = lines.map(new Function<String, String>() {
            @Override
            public String call(String line) throws Exception {
                return line.split("\t")[0] + "_" + line.split("\t")[4];
            }
        });

        //根据"ip_website"去重,然后按照website计数，转成KV格式的RDD
        JavaPairRDD<String, Integer> pairWebsiteRdd = IpWebsiteRdd.distinct().mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String ipwebsite) throws Exception {
                return new Tuple2<>(ipwebsite.split("_")[1], 1);
            }
        });

        List<Tuple2<String, Integer>> UvResultSortedByAmountDesc = pairWebsiteRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.swap();
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return tuple.swap();
            }
        }).take(5);

        for(Tuple2<String, Integer> t: UvResultSortedByAmountDesc){
            System.out.println(t);
        }
    }
}
