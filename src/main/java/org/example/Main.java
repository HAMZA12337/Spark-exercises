package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("TP1 SPARK").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Double> rdd1= sc.parallelize(Arrays.asList(12.5,10.3,14.,12.4,13.9));
        JavaRDD<Double> rdd2=rdd1.map((a)->a+1);
        JavaRDD<Double> rdd3=rdd2.filter((a)->a>10);
//        JavaRDD<Double> rdd3=rdd2.filter((a)->{
//            if(a>10) return true;
//            return false;
//        });

        List<Double> notes=rdd3.collect();

        for(double tmp :notes) {
            System.out.println(tmp);
        }







    }
}