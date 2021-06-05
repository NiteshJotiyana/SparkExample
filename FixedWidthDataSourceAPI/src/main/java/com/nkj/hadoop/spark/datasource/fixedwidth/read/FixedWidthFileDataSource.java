package com.nkj.hadoop.spark.datasource.fixedwidth.read;


import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class FixedWidthFileDataSource implements DataSourceV2, ReadSupport {


    @Override
    public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
        System.out.println("FixedWidthFileDataSource:createReader with schema : called");
        Map<String,String> map = options.asMap();
        for(String key :  map.keySet())
        {
            System.out.println("options-------"+key+" : "+map.get(key));
        }

        return new FixedWidthDataSourceReader(options,schema);
        //return ReadSupport.super.createReader(schema, options);
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {


        System.out.println("FixedWidthFileDataSource:createReader: called");
        Map<String,String> map = options.asMap();
        for(String key :  map.keySet())
        {
            System.out.println("options-------"+key+" : "+map.get(key));
        }

        return new FixedWidthDataSourceReader(options);
    }


}
