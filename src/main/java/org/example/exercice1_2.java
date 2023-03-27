package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.internal.pickling.UnPickler;

import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class exercice1_2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf();
        conf.setAppName("TP1 SPARK").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
       // first case
        JavaRDD<String> ventes = sc.textFile("ventes.txt");

        // Calcul du prix total des ventes des produits par ville pour une année donnée

        JavaPairRDD<String, Double> Rdd_prixTotalVentes = ventes.mapToPair(donnee->{
                String[] champs =donnee.split(" ");
            String ville =champs[1];
            Double prix=Double.valueOf(champs[3]);
            return new Tuple2<>(ville, prix);
        });

        // kfkfkfkk
        JavaPairRDD<String, Double> Rdd_result=Rdd_prixTotalVentes.reduceByKey((a, b) -> a + b);

        List<Tuple2<String,Double>> result=Rdd_result.collect();
        for (Tuple2<String,Double> tmp:result) {
            System.out.println(tmp._1()+" "+tmp._2());
        }

// calculer le prix total des ventes des produits par ville pour une année donnée.

        System.out.println("Pleaser Enter a year");
        Scanner sc1 = new Scanner(System.in);
        String year=sc1.nextLine();

        JavaPairRDD<Tuple3<String, String, Integer>, Double> ventesParVilleProduitAnnee = ventes.mapToPair(line -> {
            String[] tokens = line.split(" ");
            String ville = tokens[1];
            String produit = tokens[2];
            Double prix = Double.parseDouble(tokens[3]);
            Integer annee = Integer.parseInt(tokens[0].substring(0, 4));
            return new Tuple2<>(new Tuple3<>(ville, produit, annee), prix);
        });

        // filtrage des ventes pour l'année donnée
        int anneeDonnee = 2022;
        JavaPairRDD<Tuple3<String, String, Integer>, Double> ventesAnneeDonnee = ventesParVilleProduitAnnee.filter(pair -> {
            Tuple3<String, String, Integer> key = pair._1();
            return key._3() == anneeDonnee;
        });

        // calcul du prix total des ventes par ville et produit
        JavaPairRDD<Tuple2<String, String>, Double> totalVentesParVilleProduit = ventesAnneeDonnee.mapToPair(pair -> {
            Tuple3<String, String, Integer> key = pair._1;
            String ville = key._1();
            String produit = key._2();
            Double prix = pair._2();
            return new Tuple2<>(new Tuple2<>(ville, produit), prix);
        }).reduceByKey((v1, v2) -> v1 + v2);

        // affichage des résultats
        totalVentesParVilleProduit.foreach(pair -> System.out.println(pair._1() + ": " + pair._2()));


    }
}





