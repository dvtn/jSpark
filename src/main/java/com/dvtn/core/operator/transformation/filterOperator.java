package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * filter:
 *
 * public JavaRDD<T> filter(Function<T,Boolean> f): 返回RDD中满足条件的记录，为true的留下，为false的过滤掉
 */
public class filterOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("filter");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        //读取文件
        JavaRDD<String> lines = sc.textFile("./data/words");

        //按条件过滤RDD
        JavaRDD<String> filterRDD = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                return line.contains("令狐冲");
            }
        });
        //JDK1.8可以这样写
        //JavaRDD<String> filterRDD = lines.filter((line)->{
        //    return line.contains("令狐冲");
        //});

        //输出验证filterRDD
        filterRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });
        ////JDK1.8可以这样写
        //filterRDD.foreach((line)->{
        //    System.out.println(line);
        //});

        sc.close();

    }
}
