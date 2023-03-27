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

        JavaRDD<String> rdd3 =rdd2.filter((line)->line.contains("a"));
        JavaRDD<String> rdd4 =rdd2.filter((line)->line.contains("h"));
        JavaRDD<String> rdd5 =rdd2.filter((line)->line.contains("e"));
        JavaRDD<String> rdd6 =rdd3.union(rdd4);

        // map rdd 6

        JavaRDD<String> rdd7=rdd6.map((value)->value+1);












      List<String> result = rdd2.collect();

        System.out.println("this list content ");
      for(String tmp:result){
          System.out.println(tmp);
      }





    }




}
