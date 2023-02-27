package com.spark.sink;

/**
 * @author zhangxuecheng4441
 * @date 2023/2/27/027 13:59
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Properties;

public class SparkKafkaExportApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ExportDataToKafka");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> dataRdd = sc.textFile("data.txt");
        dataRdd.foreach(data -> {

            // create a KafkaProducer
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            // send data to Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>("topic", data);
            producer.send(record);
            producer.close();
        });

        sc.close();
    }
}