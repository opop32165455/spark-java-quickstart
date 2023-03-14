package com.spark.operator;

import com.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 合并两个RDD，不去重，要求两个RDD中的元素类型一致------逻辑上抽象的合并
 *
 * @author zhangxuecheng4441
 * @date 2023/3/14/014 11:30
 */
public class Union {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtils.getLocalSparkContext(Union.class);

        union(sc);
    }

    static void union(JavaSparkContext sc ) {
        List<String> data1 = Arrays.asList("张三", "李四");
        List<String> data2 = Arrays.asList("tom", "gim");

        JavaRDD<String> data1dd = sc.parallelize(data1);
        JavaRDD<String> data2Rdd = sc.parallelize(data2);

       //
       // ====================================================================
       //  |合并两个RDD，不去重，要求两个RDD中的元素类型一致                                                                            |
       //  |Merge two RDD, -not heavy, and require the consistency of the element types in the two RDD |                                                                                                                                                                                                                                    |
       //  ====================================================================
       //
        JavaRDD<String> unionRdd = data1dd
                .union(data2Rdd);

        unionRdd.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 4144225403801098130L;

            @Override
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

        sc.close();
    }
}
