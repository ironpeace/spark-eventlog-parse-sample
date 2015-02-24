package com.teppeistudio

import java.io._
import scala.io.Source
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object SparkEventLogParseSample {

    def main(args: Array[String]) = {
	    if (args.length < 1 ) printUsageLv1

    	args(0) match {
    		case "WordCount" => wordCount()
    		case "EventLogParser" => parseEventLog(args)
    		case _ => printUsageLv1
    	}
    }

    def wordCount() = {
		val conf = new SparkConf().set("spark.eventLog.enabled", "true")
		val sc = new SparkContext("local", "SparkEventLogParseSample", conf)

		val file = sc.textFile("word-count-target.txt")
		val counts = file.flatMap(line => line.split(" "))
		                 .map(word => (word, 1))
		                 .reduceByKey(_ + _)
		counts.saveAsTextFile("count-result")

		sc.stop
    }

    def parseEventLog(args: Array[String]) = {
		if (args.length < 2 ) printUsageLv2

		val path = args(1)
		val path2 = path + "_tmp"
		val path3 = path + "_Parsed.csv"

		val source = Source.fromFile(path)
		val srcStr = source.toList.mkString.replace(" at ","_at_").replace(" ","")
		source.close

		val writer2 = new PrintWriter(new File(path2))
	    writer2.write(srcStr)
	    writer2.close()

		val conf = new SparkConf()
		val sc = new SparkContext("local", "SparkEventLogParseSample", conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		import sqlContext.createSchemaRDD

		val eventlog = sqlContext.jsonFile(path2)

		//debug
		eventlog.printSchema()

		eventlog.registerTempTable("eventlog")

		val jobs:Seq[(Int, Int)]
			= sqlContext.sql("""
				SELECT Event, JobID, StageIDs 
				FROM eventlog 
				WHERE Event = 'SparkListenerJobStart'
				""")
			.map(j => for (s <- j(2).asInstanceOf[Seq[Int]]) yield {(j(1).asInstanceOf[Int], s)})
			.reduce((r1, r2) => r1 ++ r2)

		val stages:RDD[(Int, String, Long)]
			= sqlContext.sql("""
				SELECT StageInfo.StageID, StageInfo.StageName, StageInfo.SubmissionTime, StageInfo.CompletionTime 
				FROM eventlog 
				WHERE StageInfo.StageID is not NULL AND StageInfo.SubmissionTime is not NULL AND StageInfo.CompletionTime is not NULL
				""")
			.map(s => (s(0).asInstanceOf[Int], s(1).asInstanceOf[String], (s(3).asInstanceOf[Long] - s(2).asInstanceOf[Long])))

		val job_and_stages:RDD[(Int, Int, String, Long)]
			= stages.map{ s => 
				val job = jobs.filter(j => j._2 == s._1)(0)
				(job._1, s._1, s._2, s._3)
			}

		val jsStringList:Seq[String] =  
			for(js <- job_and_stages.collect) yield (js._1 + "," + js._2 + "," + js._4 + "," + js._3)

		val jsStringListWithHeader:Seq[String] = Seq("JobID,StageID,Time,StageName") ++ jsStringList

		val writer3 = new PrintWriter(new File(path3))
	    writer3.write(jsStringListWithHeader.mkString("\n"))
	    writer3.close()

	    jsStringListWithHeader.foreach(j => println(j.split(",").mkString("\t")))

		sc.stop

    }

    protected def printUsageLv1 {
        println(
            """
            Usage: sbt 
                > run command args...
            Commands:
            	WordCount
            	EventLogParser
            """)
		sys.exit(0)
    }

    protected def printUsageLv2 {
        println(
            """
            Usage: sbt 
                > run EventLogParser log-file-path
            ex: 
            sbt 'run EventLogParser /tmp/spark-events/local-1424675153544/EVENT_LOG_1
            """)
		sys.exit(0)
    }


}