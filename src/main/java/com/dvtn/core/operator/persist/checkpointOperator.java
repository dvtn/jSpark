package com.dvtn.core.operator.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * checkpoint:
 *      void checkpoint(): 标记RDD做checkpointing，它将数据存储在使用SparkContext设置的Checkpoint目录中。CheckPoint以后，将切断与父RDD的依赖关系。
 *          checkpoint功能是在job执行完以后从后往前回溯，看哪些RDD做了checkpoint标记，然后重新启动一个job，计算并做checkpoint。因此，强烈建议在对RDD做
 *          checkpoint前先对RDD做cache。
 *
 *
 * cache, persist, checkpoint持久化的单位都是partition,都是懒执行算子
 */
public class checkpointOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("checkpoint");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("WARN");

        /**
         * 实际应用中，一般将checkpoint放在HDFS里面
         * 该目录不需要预先创建
         */
        //sc.setCheckpointDir("hdfs://node01:9000/spark/checkpoint");
        sc.setCheckpointDir("./checkpoint");

        //读取数据
        JavaRDD<String> lines = sc.textFile("./data/words");
        //对该lines做checkpoint，该算子也是懒执行算子，需要action算子触发
        lines.cache();
        lines.checkpoint();

        lines.collect();

        sc.stop();

    }
}
