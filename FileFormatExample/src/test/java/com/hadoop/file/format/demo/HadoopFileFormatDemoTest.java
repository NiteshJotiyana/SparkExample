package com.hadoop.file.format.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HadoopFileFormatDemoTest {

    HadoopFileFormatDemo demo = new HadoopFileFormatDemo();

    @Before
    public void init() throws Exception {
        demo.init();
    }

    @Test
    public void testReadCSVFile() {
        String format = "csv";
        String inputFile = "/Users/nitesh/IdeaProjects/SparkProjects/HadoopFileFormatDemo/input/input.csv";
        String outputPath = "/Users/nitesh/IdeaProjects/SparkProjects/HadoopFileFormatDemo/output/output" + "_" + format;


        Dataset<Row> ds = demo.readFile(inputFile, format);

        demo.writeFile(ds, format, outputPath);


    }

    @Test
    public void testReadParquetFile() {
        String format = "parquet";
        String inputFile = "/Users/nitesh/IdeaProjects/SparkProjects/HadoopFileFormatDemo/input/input.parquet";
        String outputPath = "/Users/nitesh/IdeaProjects/SparkProjects/HadoopFileFormatDemo/output/output" + "_" + format;


        Dataset<Row> ds = demo.readFile(inputFile, format);
        demo.writeFile(ds, format, outputPath);


    }

    @Test
    public void testReadJsonFile() {
        String format = "json";
        String inputFile = "/Users/nitesh/IdeaProjects/SparkProjects/HadoopFileFormatDemo/input/input.json";
        String outputPath = "/Users/nitesh/IdeaProjects/SparkProjects/HadoopFileFormatDemo/output/output" + "_" + format;


        Dataset<Row> ds = demo.readFile(inputFile, format);

        demo.writeFile(ds, format, outputPath);


    }
}