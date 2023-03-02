package com.saprk;

import cn.hutool.core.collection.CollUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * ./bin/spark-submit  --class SparkStartApp\
 * --master yarn \
 * --conf spark.eventLog.dir=hdfs:///spark-history \
 * --deploy-mode cluster \
 * --driver-memory 2g \
 * --executor-memory 2g \
 * --executor-cores 1 \
 * --queue default \
 * /tmp/spark-app-jar-with-dependencies.jar \
 * 10
 *
 * @author zhangxuecheng4441
 * @date 2022/9/9/009 10:41
 */
public class SparkStartApp {

    public static void main(String[] args) {
        // 创建SparkConf对象
        SparkConf conf = new SparkConf().setAppName("QuickStart").setMaster("local[2]");
        // 创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddStr = sc.parallelize(CollUtil.newArrayList("1", "2", "3", "3", "3", "2", "2", "3"));
        //JavaRDD<String> rddStr = sc.textFile("hdfs://...");

        // 使用JavaSparkContext创建RDD
        rddStr.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                })
                .reduceByKey(Integer::sum)
                .collect()
                .forEach(System.out::println);

        // 关闭JavaSparkContext
        sc.close();
    }
}
