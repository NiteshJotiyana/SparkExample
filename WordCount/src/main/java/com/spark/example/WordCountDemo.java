package com.spark.example;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.Iterator;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.spark_partition_id;

public class WordCountDemo {

    SparkSession session = null;

    public static void main(String[] args) {
        new WordCountDemo();
    }

    public WordCountDemo() {

        runWordCount();
    }

    public  void runWordCount(){
        //required to change

        long start = System.currentTimeMillis();
        System.out.println("start time taken : "+start);
        String inputPath = "/Users/nitesh/IdeaProjects/SparkProjects/WordCountJavaDemo/input/olympix_data.csv";
        String outputPath = "/Users/nitesh/IdeaProjects/SparkProjects/WordCountJavaDemo/output/word_count.txt";
        String[] columns = {"name", "age", "country", "year", "closingDate", "Sports", "GoldMedal", "BronzeMedal",
                "SilverMedal", "TotalMedal"};

        //intialize session
        init();

        //read file
        Dataset<Row> ds1 = readFile(inputPath, columns);
        int cores = Runtime.getRuntime().availableProcessors();

        System.out.println("Core : "+cores );
        System.out.println(ds1.toJavaRDD().getNumPartitions());
        Dataset<Row> ds2 =ds1.repartition(500,col("name"));

        System.out.println(ds2.toJavaRDD().getNumPartitions());

        //ds2.groupBy("country").count().show();



        //ds1.write().partitionBy("country").csv("/Users/nitesh/IdeaProjects/SparkProjects/WordCountJavaDemo/output/partitioned.dat");
        //count word
        //Dataset<Row> ds2 = wordCount(ds1);


        //write into file
        writeFile(ds2, outputPath);

        long end = System.currentTimeMillis();

        System.out.println("time taken : "+(end-start));
        System.out.println("time taken : "+((end-start)/1000));
    }

    public void init() {
        session = SparkSession.builder().appName("SparkDemo").master("local").getOrCreate();
    }


    public Dataset<Row> readFile(String inputPath, String[] columns) {
        //read data
        Dataset<Row> ds = session.read().csv(inputPath)
                .toDF(columns);

        return ds;
    }

    public void writeFile(Dataset<Row> dataset, String outputPath) {
        //write data writing in one partition - single file
        dataset.coalesce(1).write().mode(SaveMode.Overwrite).csv(outputPath);
    }

    public Dataset<Row> wordCount(Dataset<Row> input) {
        //apply flatMap
        Dataset<String> ds3 = input.flatMap((Row row) -> Arrays.asList(row.mkString(",").split(",")).iterator(), Encoders.STRING());

        //apply groupBy
        Dataset<Row> ds4 = ds3.groupBy("value").count().toDF("word", "count");

        return ds4;
    }




    /*
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().appName("SparkDemo").master("local").getOrCreate();
        String[] columns = {"name", "age", "country", "year", "closingDate", "Sports", "GoldMedal", "BronzeMedal",
                "SilverMedal", "TotalMedal"};

        //required to change
        String inputPath = "/Users/nitesh/IdeaProjects/SparkProjects/WordCountJavaDemo/input/olympix_data.csv";
        String outputPath = "/Users/nitesh/IdeaProjects/SparkProjects/WordCountJavaDemo/output/word_count.txt";

        //read data
        Dataset<Row> ds = session.read().csv(inputPath)
                .toDF(columns);

        //apply flatMap
        Dataset<String> ds3 = ds.flatMap((Row row) -> Arrays.asList(row.mkString(",").split(",")).iterator(), Encoders.STRING());

        //apply groupBy
        Dataset<Row> ds4 = ds3.groupBy("value").count().toDF("word", "count");

        //write data writing in one partition - single file
        ds4.coalesce(1).write().mode(SaveMode.Overwrite).csv(outputPath);

    }*/


}
