package com.spark.operator;

import com.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 用于将RDD进行重分区，使用HashPartitioner。且该RDD的分区个数等于numPartitions个数。
 * 如果shuffle设置为true，则会进行shuffle。可以在Filter后进行Coalesce重分区来减少数据倾斜。
 *
 * @author zhangxuecheng4441
 * @date 2023/3/14/014 10:00
 */
public class Coalesce {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtils.getLocalSparkContext(Coalesce.class);

        coalesce(sc);
    }

    private static void coalesce(JavaSparkContext sc) {
        List<String> dataList = Arrays.asList("hi", "hello", "how", "are", "you");
        JavaRDD<String> dataRdd = sc.parallelize(dataList, 4);
        System.out.println("RDD的分区数: " + dataRdd.getNumPartitions());
        JavaRDD<String> dataRdd2 = dataRdd.coalesce(2, false);
        System.out.println("RDD的分区数: " + dataRdd2.partitions().size());
    }

}
