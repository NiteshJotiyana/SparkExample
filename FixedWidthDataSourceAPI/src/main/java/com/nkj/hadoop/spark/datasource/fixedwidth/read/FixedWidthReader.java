package com.nkj.hadoop.spark.datasource.fixedwidth.read;


import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;


public class FixedWidthReader implements InputPartitionReader<InternalRow> {

    private String path;
    private Scanner sc;
    private StructType schema;

    public FixedWidthReader(String path) {
        this.path = path;
        //read File
        try {
            sc = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public FixedWidthReader(String path, StructType schema) {
        this.schema = schema;
        this.path = path;
        //read File
        try {
            sc = new Scanner(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public boolean next() throws IOException {

        System.out.println("FixedWidthReader:next: called : " + sc.hasNext());

        return sc.hasNextLine();
    }

    @Override

    public InternalRow get() {
        System.out.println("FixedWidthReader:get: called");


        String line = sc.nextLine();
        System.out.println("get line : " + line);
        //Encoder<String> stringEncoder = Encoders.STRING();

        GenericInternalRow row = new GenericInternalRow(line.getBytes().length);

        int i=0;
        if (schema != null && schema.fields().length == 3) {
            for (StructField field : schema.fields()) {
                System.out.println(field.name() + " : " + field.metadata());

                int start = (int)field.metadata().getLong("start");
                int end = (int)field.metadata().getLong("end");

                System.out.println(field.name()+" : "+start+" "+end);

                String word = line.substring(start, end);
                row.update(i, UTF8String.fromBytes(word.getBytes()));

                i++;
            }
        }
        /*String word[] = new String[3];
        word[0] = line.substring(0, 8);
        word[1] = line.substring(9, 11);
        word[2] = line.substring(11, line.length());
        for (int i = 0; i < word.length; i++) {
            row.update(i, UTF8String.fromBytes(word[i].getBytes()));
        }*/


        return row;


    }


    @Override
    public void close() throws IOException {
        System.out.println("FixedWidthReader:close: called");
    }
}