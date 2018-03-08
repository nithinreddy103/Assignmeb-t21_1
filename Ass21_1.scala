package com.spark.streaming

import org.apache.spark.sql.SparkSession

object Ass21_1 extends App{

  val spark = SparkSession.builder.master("local").appName("spark").config("spark.sql.warehouse.dir","file:///C:/Users").getOrCreate()
  val tweets =  spark.read.json("tweet.json").registerTempTable("tweets")

  
  val hashtags = spark.sql("select id as id,entities.hashtags.text as words from tweets").registerTempTable("hashtags")

  val hashtag_word = spark.sql("select id as id,hashtag from hashtags LATERAL VIEW explode(words) w as hashtag").registerTempTable("hashtag_word")

  val popular_hashtags = spark.sql("select hashtag, count(hashtag) as cnt from hashtag_word group by hashtag order by cnt desc").show

}
