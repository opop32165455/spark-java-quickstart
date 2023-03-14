package com.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author zhangxuecheng4441
 * @date 2023/1/14/014 20:50
 */
public class SparkUtils {
    /**
     * 使用远程模式,添加集群配置文件到resources目录下
     * core-site.xml
     * hdfs-site.xml
     * yarn-site.xml
     *
     * @param clazz clazz
     * @param remoteAddress Remote Address
     * @return JavaSparkContext
     */
    public static JavaSparkContext getRemoteSparkContext(Class<?> clazz, String remoteAddress) {
        System.setProperty("HADOOP_USER_NAME", "root");
        //
        //SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
        //AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
        //
        SparkConf conf = getRemoteSparkConf(clazz, remoteAddress);
        conf.setJars(new String[]{"target/SparkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar"});
        //
        //基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
        //SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
        //
        return new JavaSparkContext(conf);
    }

    /**
     * Local Spark Context
     * @param clazz clazz
     * @return JavaSparkContext
     */
    public static JavaSparkContext getLocalSparkContext(Class<?> clazz) {
        System.setProperty("HADOOP_USER_NAME", "root");
        //
        //SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
        //AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
        //
        SparkConf conf = getLocalSparkConf(clazz);

        //
        //于SparkConf的对象可以创建出来一个SparkContext Spark上下文
        //parkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
        //
        return new JavaSparkContext(conf);
    }

    /***
     *  Get Remote Spark Conf
     * @param clazz clazz
     * @param remoteAddress Remote Address
     * @return SparkConf
     */
    public static SparkConf getRemoteSparkConf(Class<?> clazz, String remoteAddress) {
        SparkConf conf = new SparkConf().setAppName(clazz.getName());
        //todo
        conf.setMaster(remoteAddress);
        conf.set("deploy-mode", "client");
        return conf;
    }

    /***
     *  Get Local Spark Conf
     * @param clazz clazz
     * @return SparkConf
     */
    public static SparkConf getLocalSparkConf(Class<?> clazz) {
        return new SparkConf().setAppName(clazz.getName()).setMaster("local");
    }
}
