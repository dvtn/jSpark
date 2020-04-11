package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


/**
 * sortByKey:
 * public JavaPairRDD<K,V> sortByKey(boolean ascending)
 * Sort the RDD by key, so that each partition contains a sorted range of the elements.
 * Calling collect or save on the resulting RDD will return or output an ordered list of
 * records (in the save case, they will be written to multiple part-X files in the filesystem, in order of the keys).
 * 根据键对RDD进行排序
 *
 * 在scala中提供了sortBy算子可以直接对RDD按照key或者value排序，而对于Java, 只提供了sortByKey算子，所以要先将K,V进行交换，
 * 排序好以后再交换回来
 */

public class sortByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("sortByKey");

        /**
         * SparkContext 是通往集群的唯一通道
         */
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * 设置控制台日志打印的级别
         */
        sc.setLogLevel("WARN");

        /**
         * sc.textFile(): 读取数据
         * 一行一行读取文件中的数据并封装数据到RDD中
         */
        JavaRDD<String> linesRDD = sc.textFile("./data/words");

        /**
         * flatMap: 一对多，进来一行数据，出去多个单词
         * 对每一行数据按空格切分单词，并返回一个可迭代对象
         * 例如：Hello World -> ['Hello','World']
         */
        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        /**
         * mapToPair: 与scala中的map一样，将普通的RDD转换成K,V格式的RDD
         * 将RDD<String>转换成K,V格式的RDD<String, Integer>
         *  例如：["Hello","World"]  --> [("Hello",1), ("World", 1)]
         */
        JavaPairRDD<String, Integer> pairWordsRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {

                return new Tuple2<String, Integer>(word,1);
            }
        });

        /**
         * reduceByKey:
         * 1.将相同的key进行分组
         * 2.将每一组的key对应的value去按照指定逻辑进行处理
         */
        JavaPairRDD<String, Integer> reducedWords = pairWordsRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        /**
         * 为了按照单词的数量进行倒排序，需要将上面的K,V格式的RDD进行K,V置换，然后利用算子sortByKey进地排序
         */
        JavaPairRDD<Integer, String> swapKVRDD = reducedWords.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.swap();
            }
        });

        /**
         * 按照key进行排序，这里的ascending=false表示倒排序
         */
        JavaPairRDD<Integer, String> sortedRDD = swapKVRDD.sortByKey(false);

        /**
         * 排好序以后，还要将K,V再次进行置换，将K变成一个一个的单词，V为排序后的单词的数量
         */
        JavaPairRDD<String, Integer> swapKVSortedRDD = sortedRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return tuple.swap();
            }
        });


        /**
         * 调用action算子foreach进行触发执行，打印结果
         */
        swapKVSortedRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println("【单词为："+tuple._1+" 数量为："+tuple._2+"】");
            }
        });

        //关闭资源
        sc.stop();
    }
}
