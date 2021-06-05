package com.nkj.hadoop.spark.datasource.fixedwidth.write;

import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

public class FixedWidthWriterFactory<T> implements DataWriterFactory {

    private WriterOptions writerOptions;
    public FixedWidthWriterFactory(WriterOptions writerOptions)
    {
        this.writerOptions=writerOptions;

    }
    @Override
    public DataWriter createDataWriter(int partitionId, long taskId, long epochId) {

        writerOptions.setEpochId(epochId);
        writerOptions.setTaskId(taskId);
        writerOptions.setPartitionId(partitionId);

        FixedWidthWriter writer = new FixedWidthWriter( writerOptions);

        return writer;
    }
}
