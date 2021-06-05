package com.hadoop.pivot.demo;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Input
 * +---+---+-----+---+
 * |  A|  B|    C|  D|
 * +---+---+-----+---+
 * |foo|one|small|  1|
 * |foo|one|large|  2|
 * |foo|one|large|  2|
 * |foo|two|small|  3|
 * |foo|two|small|  3|
 * |bar|one|large|  4|
 * |bar|one|small|  5|
 * |bar|two|small|  6|
 * |bar|two|large|  7|
 * +---+---+-----+---+
 *
 *
 *
 *
 * Output
 * +---+---+-----+-----+
 * |  A|  B|large|small|
 * +---+---+-----+-----+
 * |foo|one|    4|    1|
 * |foo|two| null|    6|
 * |bar|two|    7|    6|
 * |bar|one|    4|    5|
 * +---+---+-----+-----+
 *
 *
 *
 */
public class PivotExample2 {
    SparkSession  session;

    public static void main(String[] args) {
    new PivotExample2();
    }

    public PivotExample2()
    {
        run();
    }


    public void run()
    {
        session    = SparkSession.builder().master("local").appName("Demo").getOrCreate();

        String filePath = "/Users/nitesh/IdeaProjects/SparkProjects/HadoopFileFormatDemo/input/unpivoted_data.csv";
        Dataset<Row> ds = readFile(filePath);
        ds.show();
        Dataset<Row> ds3 = pivotAction(ds);
        ds3.show();

    }

    public Dataset<Row> readFile(String path)
    {
        return session.read().option("delimiter",",").option("inferSchema", "true").csv(path).toDF("A",
                "B","C","D");
    }


    public Dataset<Row> pivotAction(Dataset<Row> input)
    {
        return input.groupBy("A", "B").pivot("C").sum("D");
    }
}
