package com.spark.operator;

import com.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangxuecheng4441
 * @date 2023/3/14/014 10:14
 */
public class CoGroup {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtils.getLocalSparkContext(CoGroup.class);

        coGroup(sc);
    }

    private static void coGroup(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "苹果"));
        data1.add(new Tuple2<>(2, "梨"));
        data1.add(new Tuple2<>(3, "香蕉"));
        data1.add(new Tuple2<>(4, "石榴"));

        List<Tuple2<Integer, Integer>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, 7));
        data2.add(new Tuple2<>(2, 3));
        data2.add(new Tuple2<>(3, 8));
        data2.add(new Tuple2<>(4, 3));

        List<Tuple2<Integer, String>> data3 = new ArrayList<>();
        data3.add(new Tuple2<>(1, "7"));
        data3.add(new Tuple2<>(2, "3"));
        data3.add(new Tuple2<>(3, "8"));
        data3.add(new Tuple2<>(4, "3"));
        data3.add(new Tuple2<>(4, "4"));
        data3.add(new Tuple2<>(4, "5"));
        data3.add(new Tuple2<>(4, "6"));

        //
        //  ===================================================== =========================
        //  |    Cogroup: groups the elements in the same key in each RDD into a collection of KV elements in each RDD.                   |
        //  |    Unlike reduceByKey, the elements of the same key are merged in the two RDD.                                                                   |
        //  ===============================================================================
        //  4==([石榴],[3],[3, 4, 5, 6])
        //  1==([苹果],[7],[7])
        //  3==([香蕉],[8],[8])
        //  2==([梨],[3],[3])
        sc.parallelizePairs(data1)
                .cogroup(sc.parallelizePairs(data2), sc.parallelizePairs(data3))
                .foreach(
                        (VoidFunction<Tuple2<Integer, Tuple3<Iterable<String>, Iterable<Integer>, Iterable<String>>>>) t ->
                                System.out.println(t._1 + "==" + t._2)
                );
    }

}