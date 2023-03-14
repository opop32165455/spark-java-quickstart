package com.spark.operator;

import com.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 返回两个RDD的交集--Returns the intersection of two RDD
 *
 * @author zhangxuecheng4441
 * @date 2023/3/14/014 10:45
 */
public class Intersection {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtils.getLocalSparkContext(Intersection.class);

        intersection(sc);
    }

    static void intersection(JavaSparkContext sc) {
        List<String> data1 = Arrays.asList("张三", "李四", "tom");
        List<String> data2 = Arrays.asList("tom","李四", "gim");

        //
        // =====================================
        //  |             返回两个RDD的交集                           |
        //  |             Returns the intersection of two RDD       |                                                                                                                                                                                                                                    |
        //  =====================================
        //
        JavaRDD<String> intersectionRdd = sc.parallelize(data1).intersection(sc.parallelize(data2));

        //tom
        //李四
        intersectionRdd.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 7801778040254506129L;

            @Override
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

    }

}