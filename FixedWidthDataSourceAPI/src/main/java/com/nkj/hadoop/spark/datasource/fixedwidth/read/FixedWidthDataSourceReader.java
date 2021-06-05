package com.nkj.hadoop.spark.datasource.fixedwidth.read;


import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


public class FixedWidthDataSourceReader implements DataSourceReader {

    private DataSourceOptions options;
    private StructType schema;

    public FixedWidthDataSourceReader(DataSourceOptions options) {
        this.options=options;
    }
    public FixedWidthDataSourceReader(DataSourceOptions options,StructType schema) {
        this.options=options;
        this.schema=schema;
    }

    @Override
    public StructType readSchema() {


        return this.schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {

        System.out.println("FixedWidthDataSourceReader:planInputPartitions: called");
        List<InputPartition<InternalRow>> list = new ArrayList<InputPartition<InternalRow>>();


        list.add(new FixedWidthReaderFactory( options.get("path").get(),this.schema));
        //list.add(new FixedWidthReaderFactory( options.get("path").get()));

        return list;
    }
}
