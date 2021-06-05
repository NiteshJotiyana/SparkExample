package com.nkj.hadoop.spark.datasource.fixedwidth.read;


import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;

public class FixedWidthReaderFactory implements InputPartition<InternalRow> {

    private String path;
    private StructType schema;

    public FixedWidthReaderFactory(String path) {
        this.path = path;
    }
    public FixedWidthReaderFactory(String path, StructType schema) {
        this.path = path;
        this.schema=schema;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {

        System.out.println("FixedWidthReaderFactory:createPartitionReader: called");


        return new FixedWidthReader(path,schema);

    }
}
