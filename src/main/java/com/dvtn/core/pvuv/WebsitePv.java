package com.dvtn.core.pvuv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class WebsitePv {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local").setAppName("WebsitePvApp");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        /**
         * 读取PVUV的数据，该数据是由程序生成，为了测试
         */
        JavaRDD<String> lines = sc.textFile(".//data/pvuvdata");

        /**
         * 根据网站的网址来生成K,V格式的RDD,基中K为网址，V为1
         */
        JavaPairRDD<String, Integer> pairWebsite = lines.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<String, Integer>(line.split("\t")[4], 1);
            }
        });

        /**
         * 根据网址和计数来统计每个网址被点击的总次数
         */
        JavaPairRDD<String, Integer> reducedWebsite = pairWebsite.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String, Integer>> PvResultSortedByAmountDesc = reducedWebsite.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.swap();
            } // 把K和V的翻转，以便下面调用sortByKey按照PV量倒排序
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return tuple.swap();
            }//把排好序的结果再次翻转，取前5
        }).take(5);

        //打印结果
        for(Tuple2<String, Integer> tuple: PvResultSortedByAmountDesc){
            System.out.println(tuple);
        }
    }
}
