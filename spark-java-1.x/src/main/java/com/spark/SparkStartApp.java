package com.spark;

import cn.hutool.core.collection.CollUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Collections;

/**
 *
 * ./bin/spark-submit  --class SparkStartApp\
 *     --master yarn \
 *     --conf spark.eventLog.dir=hdfs:///spark-history \
 *     --deploy-mode cluster \
 *     --driver-memory 2g \
 *     --executor-memory 2g \
 *     --executor-cores 1 \
 *     --queue default \
 *    /tmp/spark-app-jar-with-dependencies.jar \
 *     10
 *
 * @author zhangxuecheng4441
 * @date 2022/9/9/009 10:41
 */
@Slf4j
public class SparkStartApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark Java");
        conf.setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            //JavaRDD<String> rddStr = sc.textFile("file:///D:/tmp.txt");
            JavaRDD<String> rddStr = sc.parallelize(CollUtil.newArrayList("1","2","3","3","3","2","2","3"));
            long count = rddStr.count();
            System.out.println("count = " + count);
            JavaRDD<String> listStrRdd = rddStr.flatMap(new FlatMapFunction<String, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Iterable<String> call(String s) {
                    log.info("print :{}",s);
                    return Collections.singletonList(s);
                }
            });

            listStrRdd.foreach(new VoidFunction<String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public void call(String x) throws Exception {
                    System.out.println(x);
                }
            });


        } catch (Exception e) {
            e.printStackTrace();
        }

        sc.close();
    }
}
