package com.spark;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


import java.util.Objects;
import java.util.Set;

/**
./bin/spark-submit  --class SparkStartApp\
--master yarn \
--conf spark.eventLog.dir=hdfs:///spark-history \
--deploy-mode cluster \
--driver-memory 2g \
--executor-memory 2g \
--executor-cores 1 \
--queue default \
/tmp/spark-app-jar-with-dependencies.jar \
10
 *
 * @author zhangxuecheng4441
 * @date 2022/9/9/009 10:41
 */
@Slf4j
public class SparkReduceByKeyApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark Java");
        //conf.setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            //JavaRDD<String> rddStr = sc.textFile("file:///D:/system-info/下载/tmp (1).txt");
            JavaRDD<String> rddStr = sc.textFile("hdfs:///tmp/zxc/tmp.txt");

            long count = rddStr.count();
            log.error("find count = {}", count);

            val postIdRdd = rddStr.map(str -> {
                try {
                    val jsonObj = JSONUtil.parseObj(str);
                    return JSONUtil.getByPath(jsonObj, "post.id", "null");
                } catch (Exception e) {
                    log.error("Error parsing :" + e.getMessage(), e);
                    return "null";
                }
            });

            val userIdRdd = rddStr.map(str -> {
                try {
                    val jsonObj = JSONUtil.parseObj(str);
                    return JSONUtil.getByPath(jsonObj, "post.from.id", "null");
                } catch (Exception e) {
                    log.error("Error parsing :" + e.getMessage(), e);
                    return "null";
                }
            });

            JavaRDD<String> distinct = postIdRdd.distinct();
            long count2 = distinct.count();
            log.warn("post id:{}", count2);

            JavaRDD<String> userIdDist = userIdRdd.distinct();
            val count1 = userIdDist.count();
            log.warn("user id:{}", count1);


            reduceRdd(rddStr);


        } catch (Exception e) {
            e.printStackTrace();
        }

        sc.close();
    }

    private static void reduceRdd(JavaRDD<String> rddStr) {

        val userPostRdd = rddStr.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                try {
                    if (JSONUtil.isTypeJSON(s)) {
                        val jsonObj = JSONUtil.parseObj(s);
                        return new Tuple2<>(JSONUtil.getByPath(jsonObj, "post.from.id", "null"),
                                JSONUtil.getByPath(jsonObj, "post.id", "null"));
                    } else {
                        return null;
                    }

                } catch (Exception e) {
                    log.error("Error parsing :" + e.getMessage(), e);
                    return null;
                }

            }
        });
        val count3 = userPostRdd.count();
        System.out.println("count3 = " + count3);


        val userPostCountRdd = userPostRdd
                .filter(Objects::nonNull)
                .groupByKey()
                .mapToPair((PairFunction<Tuple2<String, Iterable<String>>, String, Integer>) keyTuple -> {
                    val postIds = keyTuple._2;
                    val postIdSet = CollUtil.newHashSet(CollUtil.toCollection(postIds));
                    return new Tuple2<>(keyTuple._1, postIdSet.size());
                });

        val userPostIdRdd = userPostRdd
                .filter(Objects::nonNull)
                .groupByKey()
                .mapToPair((PairFunction<Tuple2<String, Iterable<String>>, String, Set<String>>) keyTuple -> {
                    val postIds = keyTuple._2;
                    val postIdSet = CollUtil.newHashSet(CollUtil.toCollection(postIds));
                    return new Tuple2<>(keyTuple._1, postIdSet);
                });

        userPostCountRdd.collect().forEach(tuple -> {
            if (tuple != null) {
                log.warn("user count :{}-{}", tuple._1, tuple._2);
            }
        });

        userPostIdRdd.collect().forEach(tuple -> {
            if (tuple != null) {
                log.warn("user postId :{}-{}", tuple._1, tuple._2);
            }
        });
    }
}
