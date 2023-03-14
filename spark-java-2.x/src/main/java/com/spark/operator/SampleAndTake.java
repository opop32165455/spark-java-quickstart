package com.spark.operator;

import com.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 对RDD进行抽样，其中参数withReplacement为true时表示抽样之后还放回，可以被多次抽样，false表示不放回；fraction表示抽样比例；seed为随机数种子，比如当前时间戳
 *
 * @author zhangxuecheng4441
 * @date 2023/3/14/014 11:18
 */
public class SampleAndTake {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtils.getLocalSparkContext(SampleAndTake.class);

        sample(sc);
    }

    static void sample(JavaSparkContext sc) {
        List<Integer> dataList = Arrays.asList(1, 2, 3, 7, 4, 5, 8);

        JavaRDD<Integer> dataRdd = sc.parallelize(dataList);

        //
        // ======================================================================================================
        //  |随机抽样-----参数withReplacement为true时表示抽样之后还放回,可以被多次抽样,false表示不放回;fraction表示抽样比例;seed为随机数种子                       |
        //  |The random  sampling parameter withReplacement is true, which means that after sampling, it can be returned. It can be sampled many times,  |
        //  |and false indicates no return.  Fraction represents the sampling proportion;seed is the random number seed                                                               |                                                                                                                                                                                                                                           |
        //  ======================================================================================================
        //
        dataRdd.sample(false, 0.5, System.currentTimeMillis())
                .foreach(new VoidFunction<Integer>() {
                    private static final long serialVersionUID = -5345520290888312183L;

                    @Override
                    public void call(Integer sample) {
                        System.out.println("sample = " + sample);
                    }
                });

        // random take 3 elements
        dataRdd.takeSample(false, 3)
                .forEach(takeSample -> System.out.println("takeSample = " + takeSample));

        // take first 3 elements
        dataRdd.take(3)
                .forEach(take -> System.out.println("take = " + take));

        sc.close();
    }

}