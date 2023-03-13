package com.spark;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
./bin/spark-submit  --class com.spark.SparkDistinctApp \
--master yarn \
--conf spark.eventLog.dir=hdfs:///spark-history \
--deploy-mode cluster \
--driver-memory 2g \
--executor-memory 2g \
--executor-cores 1 \
--queue default \
/tmp/spark-java-2.x-jar-with-dependencies.jar \
10
 *
 * @author zhangxuecheng4441
 * @date 2022/9/9/009 10:41
 */
@Slf4j
public class SparkDistinctApp {

    public static void main(String[] args) {
        // 创建SparkConf对象
        SparkConf conf = new SparkConf().setAppName("Spark2.4 Distinct");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddStr = sc.textFile("hdfs:///tmp/zxc/friend-user_id.txt");

        // 使用JavaSparkContext创建RDD
        rddStr.flatMap(line -> Arrays.asList(line.split(",")).iterator())
                .filter(StrUtil::isNotBlank)
                .distinct()
                .saveAsTextFile("hdfs:///tmp/zxc/user_id-distinct.txt");

        // 关闭JavaSparkContext
        sc.stop();
        sc.close();
    }
}
