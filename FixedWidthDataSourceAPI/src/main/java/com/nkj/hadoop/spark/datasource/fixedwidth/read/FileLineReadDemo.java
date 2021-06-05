package com.nkj.hadoop.spark.datasource.fixedwidth.read;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class FileLineReadDemo {

    public static void main(String[] args) throws FileNotFoundException {
        Scanner sc = new Scanner(new File("/Users/nitesh/IdeaProjects/DataStructure/SparkFixedWidthProcessing/input.dat"));

        while (sc.hasNext())
        {
            System.out.println(sc.nextLine());
        }

    }
}
