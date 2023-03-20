package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class exercice1_1 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("first exercise TP Spark").setMaster("local[*]");
        JavaSparkContext sc= new JavaSparkContext(conf);
        // create the input RDD by parallelizing a list of strings
        List<String> lines = Arrays.asList("hello world", "foo bar", "hello spark");
        JavaRDD<String> rdd1 = sc.parallelize(lines);

        //use flatMap to split each line into words

      JavaRDD<String> rdd2=rdd1.flatMap((line)->Arrays.asList(line.split(" ")).iterator());

        JavaRDD<String> rdd3 =rdd2.filter("")













      List<String> result = rdd2.collect();

        System.out.println("this list content ");
      for(String tmp:result){
          System.out.println(tmp);
      }





    }




}
