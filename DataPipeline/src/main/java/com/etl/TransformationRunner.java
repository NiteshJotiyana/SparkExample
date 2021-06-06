package com.etl;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import java.util.HashMap;

import static org.apache.spark.sql.functions.max;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TransformationRunner {

	public static void main(String[] args) {

		SparkSession session = SparkSession.builder().appName("SparkDemo").master("local").getOrCreate();
		String[] client = { "client_id", "name", "age", "country", "region", "joining_date", "RELN" };

		String[] account = { "acc_no", "type", "start_data", "banker_id", "state", "reln" };

		String[] portfolio = { "acc_no", "equity_name", "equity_id", "unit" };

		String[] stock = { "equity_name", "id", "price" };

		Dataset<Row> clientDS = session.read().option("delimiter", "|").csv("./data/client.txt").toDF(client);

		Dataset<Row> accountDS = session.read().option("delimiter", "|").csv("./data/account.txt").toDF(account);

		Dataset<Row> portfolioDS = session.read().option("delimiter", "|").csv("./data/account_portfolio.txt")
				.toDF(portfolio);

		Dataset<Row> stockPriceDS = session.read().option("delimiter", "|").csv("./data/stock_price.txt").toDF(stock);

		HashMap<String, Integer> H = new HashMap<String, Integer>();
		H.put("SBI", 210);
		H.put("IRCTC", 1400);
		H.put("ICICI", 345);
		H.put("TESLA", 3000);
		H.put("TESLA", 3000);
		H.put("JIO", 2000);

		Dataset<Row> joined = clientDS.join(accountDS, "reln");
		Dataset<Row> result = joined.join(portfolioDS, "acc_no");
		Dataset<Row> D1 = result.join(stockPriceDS, "equity_name").withColumn("value",
				(col("unit").cast("Integer")).multiply(H.get(col("equity_name").cast("String"))));

		Dataset<Row> D2=	D1.filter("country!='Pakistan'");
		
		D1.show();
		Dataset<Row> D3 = D2.groupBy("client_id").agg(sum("value").alias("AUM"), max("name"), max("country"),
				max("region"));

		
		D3.show();
		// D1.show();

		// clientDS.show();
		// accountDS.show();
		// portfolioDS.show();

	}
}
