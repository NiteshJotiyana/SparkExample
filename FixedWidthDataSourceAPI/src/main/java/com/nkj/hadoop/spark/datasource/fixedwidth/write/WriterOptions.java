package com.nkj.hadoop.spark.datasource.fixedwidth.write;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class WriterOptions implements Serializable {

    private String writeUUID;
    private StructType schema;
    private SaveMode mode;
    private String outputFilePath;
    int partitionId;
    long taskId;
    long epochId;

    public WriterOptions(String writeUUID, StructType schema, SaveMode mode, String outputFilePath) {
        this.writeUUID = writeUUID;
        this.schema = schema;
        this.mode = mode;
        this.outputFilePath = outputFilePath;
    }

    public void setWriteUUID(String writeUUID) {
        this.writeUUID = writeUUID;
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

    public void setMode(SaveMode mode) {
        this.mode = mode;
    }

    public void setOutputFilePath(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    public String getWriteUUID() {
        return writeUUID;
    }

    public StructType getSchema() {
        return schema;
    }

    public SaveMode getMode() {
        return mode;
    }

    public String getOutputFilePath() {
        return outputFilePath;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getTaskId() {
        return taskId;
    }

    public long getEpochId() {
        return epochId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public void setEpochId(long epochId) {
        this.epochId = epochId;
    }
}
