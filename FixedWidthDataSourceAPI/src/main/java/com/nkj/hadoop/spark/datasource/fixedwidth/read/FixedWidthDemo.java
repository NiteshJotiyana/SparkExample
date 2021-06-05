package com.nkj.hadoop.spark.datasource.fixedwidth.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FixedWidthDemo {

    SparkSession session;
    public static void main(String[] args) {
        new FixedWidthDemo();
    }

    public FixedWidthDemo(){

       run();
    }

    public void run()
    {
        session    = SparkSession.builder().master("local").appName("Demo").getOrCreate();



        StructType schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.fromJson("{\"start\":0,\"end\":8}")),
                new StructField("age", DataTypes.StringType, false, Metadata.fromJson("{\"start\":8,\"end\":11}")),
                new StructField("city", DataTypes.StringType, false, Metadata.fromJson("{\"start\":11,\"end\":18}"))
        });

        //read fixedwidth file
        String inputFilePath = "/Users/nitesh/workspace/SparkFixedWidthDataSource/input/input.dat";
        Dataset<Row> ds = session.read().schema(schema).format("com.nkj.hadoop.spark.datasource.fixedwidth.read.FixedWidthFileDataSource").load(inputFilePath);



        ds.show();
        System.out.println(ds.count());
       // ds.show();
    }


}
