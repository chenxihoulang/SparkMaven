package com.chw.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

import scala.Tuple2;

public class SparkWC {
    public static void main(String[] args) {
        System.out.println("spark word count ...");
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("test");

        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaPairRDD<String, Integer> nameRdd = sc.parallelizePairs(
                Arrays.asList(new Tuple2<String, Integer>("zhangsan", 10),
                        new Tuple2<String, Integer>("lisi", 20), new Tuple2<String, Integer>("maliu", 12)));
        final JavaPairRDD<String, Integer> socreRdd = sc.parallelizePairs(
                Arrays.asList(new Tuple2<String, Integer>("zhangsan", 100),
                        new Tuple2<String, Integer>("lisi", 200), new Tuple2<String, Integer>("wangwu", 300)));

//        final JavaPairRDD<String, Tuple2<Integer, Integer>> joinRdd = nameRdd.join(socreRdd);
//        joinRdd.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
//                System.out.println(stringTuple2Tuple2);
//            }
//        });

        final JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> lefJoinRdd = nameRdd.leftOuterJoin(socreRdd);
        lefJoinRdd.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);

                final Optional<Integer> integerOptional = stringTuple2Tuple2._2._2;

                if (integerOptional.isPresent()) {
                    System.out.println(integerOptional.get());
                } else {
                    System.out.println("空值");
                }
            }
        });
    }

    public String getName() {
        return SparkWC.class.getSimpleName();
    }
}
