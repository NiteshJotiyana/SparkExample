package com.spark.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MathCalculationDemo {

    SparkSession session;

    public static void main(String[] args) {
      MathCalculationDemo M = new MathCalculationDemo();
      M.run();
    }

    public MathCalculationDemo() {

    }

    public void run()
    {
        String inputFile = "/Users/nitesh/IdeaProjects/SparkProjects/WordCountJavaDemo/input/math_data.txt";

        init();
        Dataset<Row> ds1 = readFile(inputFile);
        Dataset<Integer> ds3 = calculation(ds1);


        ds3.show();
    }
    public Dataset<Integer> calculation(Dataset<Row> input) {

        //apply flatMap
        MapFunction<Row,Integer> m = (Row row)->{

            List<String> A = Arrays.asList(row.mkString(",").split(","));

            int sum=0;
            for(String e :  A)
            {
                sum+=Integer.parseInt(e);
            }
            return sum;
        };
        Dataset<Integer> ds3 = input.map(m, Encoders.INT());

        return ds3;

    }

    public void init() {
        session = SparkSession.builder().appName("SparkDemo").master("local").getOrCreate();
    }


    public Dataset<Row> readFile(String inputPath) {
        //read data
        Dataset<Row> ds = session.read().csv(inputPath)
                .toDF();

        return ds;
    }
}
