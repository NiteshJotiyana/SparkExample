package com.hadoop.pivot.demo;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Input
 * +---+---------------+
 * | id|          marks|
 * +---+---------------+
 * |  1| eng:50,math:20|
 * |  2| eng:75,math:70|
 * |  3|eng:100,math:90|
 * |  4| eng:50,math:60|
 * +---+---------------+
 *
 *
 * Output
 * +---+---+----+
 * | id|eng|math|
 * +---+---+----+
 * |  1| 50|  20|
 * |  2| 75|  70|
 * |  3|100|  90|
 * |  4| 50|  60|
 * +---+---+----+
 *
 */
public class SplitColumnExample {
    SparkSession  session;

    public static void main(String[] args) {
    new SplitColumnExample();
    }

    public SplitColumnExample()
    {
        run();
    }


    public void run()
    {
        session    = SparkSession.builder().master("local").appName("Demo").getOrCreate();

        String filePath = "/Users/nitesh/IdeaProjects/SparkProjects/HadoopFileFormatDemo/input/marks.csv";
        Dataset<Row> ds = readFile(filePath);
        ds.show();
       Dataset<Row> ds2 =converrowToColumn(ds);
        ds2.show();
        Dataset<Row> ds3 = pivotAction(ds);
        ds3.show();

    }

    public Dataset<Row> readFile(String path)
    {
        return session.read().option("delimiter","|").csv(path).toDF("id",
                "marks");
    }

    public Dataset<Row> converrowToColumn(Dataset<Row> input)
    {

        //splitting column based on ,
        input  = input.withColumn("_tmp",split(col("marks"),","));

        //splitting column based on ,
        input = input.select(col("id"),col("_tmp").getItem(0).as("eng"),col("_tmp").getItem(1).as("math"));

        //splitting column based on :
        input = input.select(col("id"),split(col("eng"),":").getItem(1).as("eng"),split(col("math"),":").getItem(1).as("math"));
        return input;

        //return input.groupBy("name","age").pivot("city").agg(count("city"));
    }

    public Dataset<Row> pivotAction(Dataset<Row> input)
    {
        return input.groupBy("marks","id").pivot("marks").agg(count("marks"));
    }
}
