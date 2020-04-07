package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * public JavaRDD<T> sample(boolean withReplacement, double fraction, long seed): 对于指定的RDD按比例与(种子)进行抽样,返回抽样后原RDD的一个子集。
 * 参数说明：
 *  withReplacement: boolean类型，必要参数，表示有无放回抽样，为true表示有放回抽样，为false时表示无放回抽样。
 *  fraction: double类型，必要参数，表示抽样的比例，取值空间为[0,1]
 *  seed: long类型，可选参数。如果在其他参数不变的情况下固定该种子，每次抽样得到的样本是一样的。多用于程序测试。
 *
 *
 */
public class sampleOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("sample");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        //读取文件
        JavaRDD<String> lines = sc.textFile("./data/words");

        //调用sample转化算子以RDD进行抽样。
        JavaRDD<String> sampleRDD = lines.sample(true, 0.1, 100);

        //调用action算子触发上面的sample算子，打印结果
        sampleRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });

        sc.close();

    }
}
