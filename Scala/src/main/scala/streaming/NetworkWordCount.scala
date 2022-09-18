package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.time.LocalDateTime

// The main object
object NetworkWordCount {

  // The main functions
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: HdfsWordCount <input-directory> <output-directory>")
      System.exit(1)
    }

    // Setting the spark configuration
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Setting the checkpoint for updateStateByKey
    ssc.checkpoint(".")

    // Creating the FileInputDStream on the input directory
    val lines = ssc.textFileStream(args(0))

    // Creating an array of all the words present in the new file
    val words = lines.flatMap(_.split(" "))

    // Filtering the words which are present only of allowed characters and assigning a single frequency to each word
    val words2 = words.filter(_.matches("[a-zA-Z]+")).map(x=>(x,1))

    // Assigning the words for task 2 & 3
    // Creating an array of words present in a line for each line, each word must only consist of characters and should be greater than length 5.
    val wordsForTask3 = lines.map(x => x.split(" ")).map(x=>x.filter(_.matches("[a-zA-Z]{5,}")))

    // For each line, an array of words is created for each line, now the co-occurred word would be present before and after 5 places of the chosen word
    // Those words would be clubbed together with the chosen word and given a frequency of 1.
    // For example, a line with words:- "This is a line only to be considered as an example.", the co-occurred words for "only" would be
    // This, is, a, line, to, be, considered, as
    val words3 = wordsForTask3.map( word=> for{ i<-0 until word.length;j<-0 until word.length}yield{if(i!=j){(try{("("+word(i)+","+word(j)+")",1)}catch{case e:ArrayIndexOutOfBoundsException => ("",1)})}else{("",1)}}).flatMap(_.toList).filter(_!=("",1))

    // The for loop takes helps to target a single word in the array of words, to ensure the  same words aren't considered, the if condition is used,
    // try-catch is used to cater to the problem in which the array index reaches out of bounds, the whole is flattened and empty clauses put in case of
    // catch or else condition are filtered out.

    // The output for task 1
    val wordCounts = words2.reduceByKey(_ + _)

    // Task 2
    val task2 = words3.reduceByKey(_+_)

    // Task 3
    val state = words3.updateStateByKey((values: Seq[(Int)], state: Option[(Int)]) => {
      var value = state.getOrElse(0)
      values.foreach(i => {
        value += i
      })
      Some(value)})



    wordCounts.foreachRDD(rdd => {
      if(rdd.count > 0) {
        val fileName = LocalDateTime.now().toString().replace(":","_").replace("-","_")
        rdd.saveAsTextFile(args(1)+"task1/"+fileName+"")
      }
    })

    // Storing task 2
    task2.foreachRDD(rdd => {
      if(rdd.count > 0) {
        val fileName = LocalDateTime.now().toString().replace(":","_").replace("-","_")
        rdd.saveAsTextFile(args(1)+"task2/"+fileName+"")
      }
    })

    // Storing task 3
    state.foreachRDD(rdd => {
      if(rdd.count > 0) {
        val fileName = LocalDateTime.now().toString().replace(":","_").replace("-","_")
        rdd.saveAsTextFile(args(1)+"task3/"+fileName+"")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}