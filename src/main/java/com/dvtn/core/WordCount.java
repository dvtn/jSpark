package com.dvtn.core;

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
 * Java版本的WordCount
 */
public class WordCount {
    public static void main(String[] args) {

        /**
         * conf:
         * 1. 可设置Spark的运行模式
         *    Spark有以下几种运行模式：
         *      1）local: 在idea或者eclipse中开发Spark程序用local模式，也称作本地模式，多用于测试
         *      2）standalone: Spark自带的资源调度框架，支持分布式搭建，Spark任务可以依赖standalone调度资源
         *      3) yarn: Hadoop生态圈中的资源调度框架，Spark也可以基于yarn调度资源
         *      4）mesos: 资源调度框架
         * 2. 可以设置Spark在WebUI中显示的Application的名称。
         *
         * 3. 设置当前Spark Application运行的资源配置(内存和核),通过conf.set(K,V)来设置
         */
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("SparkWordCountApp");

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
