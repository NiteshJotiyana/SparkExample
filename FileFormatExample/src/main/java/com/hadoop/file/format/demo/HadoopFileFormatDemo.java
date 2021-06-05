package com.hadoop.file.format.demo;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.Properties;

public class HadoopFileFormatDemo {

    SparkSession session;

    public HadoopFileFormatDemo() {

    }

    public void init(String sparkPropertiesFilePath) throws Exception {

        if(sparkPropertiesFilePath==null|| sparkPropertiesFilePath.isEmpty())
            throw new Exception("Invalid File Path : "+sparkPropertiesFilePath);

        Properties sparkProperties = new Properties();
        try {
            sparkProperties.load(new FileReader(new File(sparkPropertiesFilePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        SparkConf conf = new SparkConf();

        setAll(conf, sparkProperties);

        session = SparkSession.builder().config(conf).getOrCreate();
    }

    public void init() throws Exception {

        String sparkPropertiesFile = "/Users/nitesh/IdeaProjects/SparkProjects/HadoopFileFormatDemo/spark.properties";
        init(sparkPropertiesFile);

    }

    public Dataset<Row> readFile(String path, String format) {
        Dataset<Row> ds = session.read().format(format).load(path);
        ds.show();
        return ds;
    }

    /**
     * compression - none, uncompressed, snappy, gzip, lzo, brotli, lz4, zstd.
     *
     * @param dataset
     * @param format
     * @param outputPath
     */
    public void writeFile(Dataset<Row> dataset, String format, String outputPath) {

        //option("compression","gzip")
        dataset.write().format(format).mode(SaveMode.Overwrite).save(outputPath);


    }

    /**
     * Update conf opject with properties
     *
     * @param conf
     * @param properties
     */
    public void setAll(SparkConf conf, Properties properties) {
        if (conf != null && properties != null) {
            for (String key : properties.stringPropertyNames()) {
                conf.set(key, properties.getProperty(key));
            }
        }
    }
}
