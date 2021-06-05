package com.nkj.hadoop.spark.datasource.fixedwidth.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.io.FileWriter;
import java.io.IOException;

public class FixedWidthWriter implements DataWriter<InternalRow> {

    FileWriter myWriter = null;
    WriterOptions writerOptions;

    public FixedWidthWriter(WriterOptions writerOptions) {
        this.writerOptions = writerOptions;
        try {
            this.myWriter = new FileWriter(writerOptions.getOutputFilePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void write(InternalRow record) throws IOException {

        try {
            if (record != null) {

                System.out.println(record);

                StringBuilder sb = new StringBuilder("");
                for (int i = 0; i < record.numFields(); i++) {
                    String s = new String(record.getString(i));
                    System.out.println(s);

                    sb.append(s);
                    if (i + 1 < record.numFields())
                        sb.append("|");

                }

                myWriter.write(sb.toString());
                myWriter.write("\n");

                System.out.println("Successfully wrote to the file.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        myWriter.close();
        return null;
    }

    @Override
    public void abort() throws IOException {
        myWriter.close();
    }
}
