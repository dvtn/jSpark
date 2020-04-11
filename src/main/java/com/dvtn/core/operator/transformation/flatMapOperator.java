package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * flatMap:
 *
 * flatmap()是将函数应用于RDD中的每个元素，将返回的迭代器的所有内容构成新的RDD
 *
 * <U> JavaRDD<U> flatMap(FlatMapFunction<T,U> f)
 * Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
 */

public class flatMapOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("flatMap")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> lines = sc.textFile("./data/words");

        /**
         * flatMap: 一对多，进来一行数据，出去多个单词
         * 对每一行数据按空格切分单词，并返回一个可迭代对象
         * 例如：Hello World -> ['Hello','World']
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<>();
                list.add(s);
                return list.iterator();
            }
        });

        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.stop();

    }
}
