package com.olympic.da;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import scala.Predef.any2stringadd;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.api.java.function.MapFunction;

public class OlympicDARunner {

	public static void main(String[] args) {
		SparkSession session = SparkSession.builder().appName("SparkDemo").master("local").getOrCreate();
		String[] columns = { "name", "age", "country", "year", "closingDate", "Sports", "GoldMedal", "BronzeMedal",
				"SilverMedal", "TotalMedal" };

		Dataset<Row> ds = session.read().csv("/Users/nitesh/git/SparkProjects/OlympicsDaJava/olympix_data.csv")
				.toDF(columns);

		// ds.filter(row ->
		// row.getAs("country").equals("India")).groupBy("Sports").agg(sum("TotalMedal")).show();

		MapFunction<Row, Athlete> fun = new MapFunction<Row, Athlete>() {

			@Override
			public Athlete call(Row row) throws Exception {

				return new Athlete(row.getAs("name"), row.getAs("country"), row.getAs("totalMedal"));
			}
		};
		// ds.groupBy("name").agg(sum("TotalMedal").cast(DataTypes.IntegerType).as("totalMedal"),
		// max("country").as("country"))
		// .map(fun, Encoders.bean(Athlete.class)).sort("name").show();

		ds.filter(row -> row.getAs("country").equals("United States")).groupBy("name", "year")
				.agg(sum("GoldMedal").as("medal")).repartition(4).write()
				.csv("/Users/nitesh/git/SparkProjects/OlympicsDaJava/report1.csv");
	}
}
