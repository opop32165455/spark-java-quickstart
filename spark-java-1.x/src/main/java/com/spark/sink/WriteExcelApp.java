package com.spark.sink;

import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangxuecheng4441
 * @date 2023/2/27/027 11:47
 */
public class WriteExcelApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark Java");
        conf.setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //Read data from Spark
        JavaRDD<String> dataRdd = sc.textFile("hdfs://path/to/data.txt");

        //Convert data to JavaRDD<Row>
        JavaRDD<Row> rowRdd = dataRdd.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                String[] fields = line.split(",");
                return RowFactory.create(fields[0], fields[1], fields[2]);
            }
        });

        //Create a StructType specifying the data types of the columns
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("col1", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("col2", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("col3", DataTypes.StringType, true));
        val schema = DataTypes.createStructType(fields);

        //Create a DataFrame from the JavaRDD<Row>
        DataFrame df = sqlContext.createDataFrame(rowRdd, schema);

        //Write the DataFrame to an Excel file
        df.write().format("com.crealytics.spark.excel")
                .option("dataAddress", "'Sheet1'!A1")
                .option("useHeader", "true")
                .option("treatEmptyValuesAsNulls", "false")
                .option("inferSchema", "false")
                .option("addColorColumns", "true")
                .save("hdfs://path/to/output.xlsx");

        sc.close();
    }
}
