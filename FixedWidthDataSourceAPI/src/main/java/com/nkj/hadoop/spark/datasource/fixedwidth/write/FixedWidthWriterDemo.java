package com.nkj.hadoop.spark.datasource.fixedwidth.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FixedWidthWriterDemo {
    SparkSession session;
    public static void main(String[] args) {
        new FixedWidthWriterDemo();
    }

    public FixedWidthWriterDemo(){

        run();
    }

    public void run()
    {
        session    = SparkSession.builder().master("local").appName("Demo").getOrCreate();
        Dataset<Row> ds = session.read().option("delimiter",",").csv("/Users/nitesh/workspace/SparkFixedWidthDataSource/input/input.csv");
        String outputPath ="/Users/nitesh/workspace/SparkFixedWidthDataSource/output/output.csv";

        ds.show();

        ds.write().format("com.nkj.hadoop.spark.datasource.fixedwidth.write.FixedWidthDataSourceV2Write").save(outputPath);

    }
}
