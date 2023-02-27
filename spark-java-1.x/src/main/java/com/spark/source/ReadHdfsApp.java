package com.spark.source;

/**
 * @author zhangxuecheng4441
 * @date 2023/2/27/027 11:40
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadHdfsApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ReadHDFS");
        //conf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://<namenode>:<port>/<path>");
        lines.filter(line -> !line.contains("pornographic") && !line.contains("reactionary") && !line.contains("violent")).collect().forEach(System.out::println);

        sc.close();
    }
}