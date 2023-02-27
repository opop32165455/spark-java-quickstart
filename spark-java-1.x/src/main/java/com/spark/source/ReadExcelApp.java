package com.spark.source;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
/**
 * @author zhangxuecheng4441
 * @date 2023/2/27/027 11:42
 */
public class ReadExcelApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ReadExcel");
        //conf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().format("com.crealytics.spark.excel")
                .option("useHeader", "true")
                .option("treatEmptyValuesAsNulls", "true")
                .option("inferSchema", "true")
                .option("addColorColumns", "false")
                .load("hdfs://path/to/excel.xlsx");
        df.show();
    }
}