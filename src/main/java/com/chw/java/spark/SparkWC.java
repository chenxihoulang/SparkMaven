package com.chw.java.spark;

import org.apache.spark.SparkConf;

public class SparkWC {
    public static void main(String[] args) {
        System.out.println("spark word count ...");
        SparkConf conf = new SparkConf();
    }

    public String getName() {
        return SparkWC.class.getSimpleName();
    }
}
