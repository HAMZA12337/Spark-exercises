package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class persist {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf= new SparkConf().setAppName("APP3").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Double> rdd1=sc.parallelize(Arrays.asList(1.3,13.2,123.4,13.3));
//        JavaRDD<Double> rdd2=rdd1.map(adouble->adouble+1);
        JavaRDD<Double> rdd2=rdd1.map(adouble->{

            System.out.println("==map====");

            return adouble+1;});
        rdd2.cache();
        JavaRDD<Double> rdd3=rdd2.filter(aDouble -> {

            System.out.println("=========filter====================");
            return aDouble>=10;});



        List<Double> numbers=rdd3.collect();


        for (Double number:numbers ){
            System.out.println(number);
        }


        List<Double> numbers1=rdd3.collect();


        for (Double number1:numbers ){
            System.out.println(number1);
        }


    }







}
