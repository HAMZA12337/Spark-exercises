package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

     public static void main(String[] args) {

         SparkConf conf = new SparkConf();
         conf.setAppName("WORD COUNTER").setMaster("local[*]");
         JavaSparkContext sc = new JavaSparkContext(conf);
         JavaRDD<String> lines=sc.textFile("names.txt");
         JavaRDD<String> words=lines.flatMap((line)-> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer>  wordPairs= words.mapToPair((word)->new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> wordCount =wordPairs.reduceByKey((a,b)->a+b);
        List<Tuple2<String,Integer>> result=wordCount.collect();

        for(Tuple2<String,Integer> tmp :result){
            System.out.println(tmp._1()+" "+tmp._2());
        }




     }









}
