package com.nkj.hadoop.spark.datasource.fixedwidth.write;


import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Optional;

public class FixedWidthDataSourceV2Write implements DataSourceV2, WriteSupport {


    @Override
    public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {

        Map<String,String> map = options.asMap();
        for(String key :  map.keySet())
        {
            System.out.println("options-------"+key+" : "+map.get(key));
        }

        WriterOptions writerOptions = new WriterOptions(writeUUID,schema,mode,map.get("path"));
        FixedWidthDataSourceWriter writer = new FixedWidthDataSourceWriter(writerOptions);

        return Optional.of(writer);
    }
}
