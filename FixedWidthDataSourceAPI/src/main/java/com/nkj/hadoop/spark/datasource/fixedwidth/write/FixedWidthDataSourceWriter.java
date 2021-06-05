package com.nkj.hadoop.spark.datasource.fixedwidth.write;


import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;


public class FixedWidthDataSourceWriter implements DataSourceWriter {


    private WriterOptions writerOptions;
    public FixedWidthDataSourceWriter(WriterOptions writerOptions)
    {
        this.writerOptions =writerOptions;
    }
    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {

        FixedWidthWriterFactory<InternalRow> writerFactory = new FixedWidthWriterFactory(writerOptions);
        return writerFactory;
    }

    @Override
    public boolean useCommitCoordinator() {
        return DataSourceWriter.super.useCommitCoordinator();
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
        DataSourceWriter.super.onDataWriterCommit(message);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {

    }

    @Override
    public void abort(WriterCommitMessage[] messages) {

    }
}
