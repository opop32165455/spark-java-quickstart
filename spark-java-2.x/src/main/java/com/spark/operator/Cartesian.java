package com.spark.operator;

import com.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhangxuecheng4441
 * @date 2023/3/14/014 9:49
 */
public class Cartesian {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtils.getLocalSparkContext(Cartesian.class);

        cartesian(sc);
    }

    private static void cartesian(JavaSparkContext sc) {
        List<String> names = Arrays.asList("张三", "李四", "王五");
        List<Integer> scores = Arrays.asList(60, 70, 80);

        JavaRDD<String> namesRdd = sc.parallelize(names);
        JavaRDD<Integer> scoreRdd = sc.parallelize(scores);

        //
        //  *  =====================================
        //  *   |             两个RDD进行笛卡尔积合并                                        |
        //  *   |             The two RDD are Cartesian product merging     |                                                                                                                                                                                                                                    |
        //  *   =====================================
        //  */
        JavaPairRDD<String, Integer> cartesianRdd = namesRdd.cartesian(scoreRdd);

        //张三	60
        //张三	70
        //张三	80
        //李四	60
        //李四	70
        //李四	80
        //王五	60
        //王五	70
        //王五	80
        cartesianRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> t) {
                System.out.println(t._1 + "\t" + t._2());
            }
        });
    }

}