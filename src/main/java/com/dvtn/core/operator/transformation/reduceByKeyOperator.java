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
 * reduceByKey:
 * public JavaPairRDD<K,V> reduceByKey(Function2<V,V,V> func)
 * Merge the values for each key using an associative and commutative reduce function.
 * This will also perform the merging locally on each mapper before sending results to a reducer,
 * similarly to a "combiner" in MapReduce.
 * Output will be hash-partitioned with the existing partitioner/ parallelism level.
 *
 * 1.将相同的key进行分组
 * 2.将每一组的key对应的value去按照指定逻辑进行处理
 *
 * reduceByKey是shuffle类的算子，可以手动设置生成的RDD分区数以提高或减少并行度。
 */

public class reduceByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("reduceByKey");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> lines = sc.textFile("./data/words");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        /**
         * 对文件中切分的单词计数为1
         */
        JavaPairRDD<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        /**
         * 使用reduceByKey转换算子统计文件中切分出的单词的数量
         */

        JavaPairRDD<String, Integer> reduceRDD = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reduceRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple);
            }
        });

        sc.stop();
    }
}
