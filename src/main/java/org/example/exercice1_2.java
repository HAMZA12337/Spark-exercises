//package org.example;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import scala.Tuple2;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
//public class exercice1_1 {
//    public static void main(String[] args) {
//        Logger.getLogger("org").setLevel(Level.OFF);
//        SparkConf conf = new SparkConf();
//        conf.setAppName("TP1 SPARK").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        // first case
//        JavaRDD<String> ventes = sc.textFile("ventes.txt");
//
//        // Calcul du prix total des ventes des produits par ville pour une année donnée
//
//        JavaPairRDD<String, Double> Rdd_prixTotalVentes1 = ventes.filter((donnee)-> {
//            String[] champs =donnee.split(" ");
//            String ville =champs[1];
//        });
//                }
//        JavaPairRDD<String, Double> Rdd_prixTotalVentes2 = ventes.mapToPair(donnee->{
//            String[] champs =donnee.split(" ");
//
//
//            String ville =champs[1];
//            Double prix=Double.valueOf(champs[3]);
//            return new Tuple2<>(ville, prix);
//        });
//
//        JavaPairRDD<String, Double> Rdd_result=Rdd_prixTotalVentes.reduceByKey((a, b) -> a + b);
//
//        List<Tuple2<String,Double>> result=Rdd_result.collect();
//        for (Tuple2<String,Double> tmp:result) {
//            System.out.println(tmp._1()+" "+tmp._2());
//        }
//
//
//    }
//}
//
//
//
//
//
