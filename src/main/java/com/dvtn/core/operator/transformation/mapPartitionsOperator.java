package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * mapPartitions
 *  <U> JavaRDD<U> mapPartitions(FlatMapFunction<java.util.Iterator<T>,U> f,boolean preservesPartitioning)
 *      Return a new RDD by applying a function to each partition of this RDD.
 *      功能和map相似，只是处理数据的时候是按照分区一个一个处理，尤其是在处理类似数据库操作的时候，
 *      连接数据库，执行操作，关闭数据库连接 => 每个分区只要连接一次数据库和关闭一次数据库，提高了处理的效率与减少了数据库的频繁连接与关闭操作
 */
public class mapPartitionsOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("mapPartitions");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(
                "刘备", "关羽", "张飞", "诸葛亮",
                "曹操", "张辽", "典韦", "郭嘉",
                "孙权", "周瑜", "甘宁", "诸葛瑾"
        ), 3);

        //如果是使用map的传统操作，对于每一个元素的操作都要执行一次, 在对数据库操作时，会频繁地创建与关闭数据库，增加数据库的压力。
        //当数据量特别大时，有可能让数据库崩掉

        /**
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         */
        JavaRDD<String> mapRDD = rdd1.map(new Function<String, String>() {

            @Override
            public String call(String s) throws Exception {

                System.out.println("创建数据库连接.....");
                System.out.println("拼接数据库DML语句，执行对数据表的crud操作....");
                System.out.println("关闭数据库连接");

                return s;
            }
        });

        /**
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         * 创建数据库连接.....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 拼接数据库DML语句，执行对数据表的crud操作....
         * 关闭数据库连接
         */

        JavaRDD<String> mapPartitionsRDD = rdd1.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> itr) throws Exception {
                System.out.println("创建数据库连接.....");
                List<String> list = new ArrayList<>();
                while (itr.hasNext()) {
                    String value = itr.next();
                    list.add("insert into 表名(字段名) values (" + value + "); \n");
                    System.out.println("拼接数据库DML语句，执行对数据表的crud操作....");
                }
                System.out.println("关闭数据库连接");
                return list.iterator();
            }
        });
        mapRDD.collect();
        mapPartitionsRDD.collect();

        sc.stop();

    }
}
