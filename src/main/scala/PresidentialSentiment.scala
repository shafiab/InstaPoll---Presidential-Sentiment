/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.shafiab

import java.net._
import java.io._
import scala.io._

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import java.util.Properties;
import scala.collection.JavaConversions._

object PresidentialSentiment{
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: PresidentialSentiment <hostname> <port>")
      System.exit(1)
    }

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("PresidentialSentiment")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("./checkpoint/") 

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");

    val tweet_stream = lines.map( x=> parse(x))


    
    val mapper_candidate_counts =  tweet_stream.map(
        tweet => 
        ( 
            compact(render(tweet \ "candidate_name")),
            1,
            get_sentiment( compact(render(tweet \ "text")), props )
            )
        )


    val mapper_states_candidates = tweet_stream.map(
        tweet =>
        ( compact(render(tweet \ "state")), 
            compact(render(tweet \ "candidate_name")),
            1) )
    val mapper_states_candidates2 = mapper_states_candidates.filter{case(a,b,c)=> !a.contains("none")}
    val mapper_states_candidates1 = mapper_states_candidates2.map{case(a,b,c) => ((a,b),c)}
    val mapper_candidate_counts1 = mapper_candidate_counts.map{case(a,b,(c,d,e)) => (a, (b,c,d,e)) }

    //mapper_candidate_counts1.print()

    val candidate_counts_sentiments = mapper_candidate_counts1.reduceByKeyAndWindow(
        (a :(Int,Int,Int,Int), b:(Int,Int,Int,Int)) => (a._1 + b._1, a._2 + b._2, a._3+b._3, a._4+b._4), Seconds(60), Seconds(10))
    val state_candidate_count = mapper_states_candidates1.reduceByKeyAndWindow(
        (a:Int, b:Int) => a+b, Seconds(60), Seconds(10)
        )

    candidate_counts_sentiments.print()
    state_candidate_count.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def get_sentiment(tweet: String, props: Properties) : (Int,Int, Int) = {
    val pipeline = new StanfordCoreNLP(props);
    val annotation = pipeline.process(tweet);
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    val trees = sentences.map( sentence => sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree]) ) 
    val sentiments = trees.map( tree => RNNCoreAnnotations.getPredictedClass(tree) )
    val sentiments1 = sentiments.map(x => x-2) 
    return (sentiments1.max , bool2int(sentiments1.max >0), bool2int(sentiments1.max == 0))
  }  

  def bool2int(b:Boolean) = if (b) 1 else 0
}
