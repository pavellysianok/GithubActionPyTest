// Databricks notebook source
// MAGIC %md
// MAGIC # imports and vars

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import org.apache.hadoop.fs.{Path => HadoopPath}

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType, TimestampType, BinaryType, DateType}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import java.sql.Timestamp

// COMMAND ----------

//RAW
val basePath = dbutils.widgets.get("basePath")
val lccProdDataPostfix = dbutils.widgets.get("lccProdDataPostfix")
val lccProdData= s"$basePath/$lccProdDataPostfix"
//BACKUP
val lccBackupPath = dbutils.widgets.get("lccBackupPath")

val tmpProdFolderPostfix = dbutils.widgets.get("tmpProdFolderPostfix")
val tmpProdFolder = s"$basePath/$tmpProdFolderPostfix"

val restoreOutputPath, diffPath = s"${tmpProdFolder}_tmp"
val metricsPath = s"${tmpProdFolder}_metrics"

val daysToCheckStr = dbutils.widgets.get("daysToCheck")

// COMMAND ----------

import java.sql.Timestamp
case class LCCParsed(macId: String, timestamp: Timestamp, firstJSON: String, secondJSON: String)

case class LCCKey(macId: String, pubTime: String)

case class LCCConvertedDF(MacId: String, EventTime: Timestamp, EnqueuedTime: Timestamp, MessageProperties: String,
                          MessageData: String, Type: String, ParsedType: String, Seq: String, SessionId: String,
                          Payload: Array[Byte])

// COMMAND ----------

val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
val daysToCheck = daysToCheckStr.split(",").map(LocalDate.parse(_, dateFormat))

// COMMAND ----------

val lccPattern = raw"(\w+) ([\d\-T\:\.Z]+)(\{.+?\})(\{.+\})".r
val pubPattern = """\"PublishTime\":\"([\d\-T\:]+).*\"""".r

def parseLCC = udf((text: String) => {
    text.trim match {
        case lccPattern(macId, rawTimestamp, firstJSON, secondJSON) => {
            try { Some(LCCParsed(macId, Timestamp.from(ZonedDateTime.parse(rawTimestamp).toInstant), firstJSON, secondJSON)) }
            catch { case _ : Throwable => None }
        }
        case _ => None
    }
})

val getLCCKey = udf((macId: String, propJSON: String) => {
    LCCKey(macId, pubPattern.findFirstMatchIn(propJSON).map(_.group(1)).getOrElse(null))
})

val getLCCConvertedDF = udf((macId: String, timestamp: Timestamp, firstJSON: String, secondJSON: String, messageType: String) => {
    LCCConvertedDF(
        MacId = macId,
        EventTime = timestamp,
        EnqueuedTime = timestamp,
        MessageProperties = firstJSON,
        MessageData = secondJSON,
        Type = messageType,
        ParsedType = messageType,
        Seq = null,
        SessionId = null,
        Payload = null
    )
})


val getPartitionDateKey = udf((timestamp: Timestamp) => {
    timestamp.toLocalDateTime().toLocalDate().toString()
})

// COMMAND ----------

val dlFS = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(basePath), spark.sparkContext.hadoopConfiguration)
val blobFS = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(lccBackupPath), spark.sparkContext.hadoopConfiguration)

// COMMAND ----------

//Extract all available events from backup path
val allMsgs = blobFS.listStatus(new HadoopPath("/")).map(_.getPath.getName)

// COMMAND ----------

// MAGIC %md
// MAGIC # 2 STEPS IN ONE! '1 Identifies missing data' + '2 Extracts diffs'

// COMMAND ----------

val recordsPerPartition = 2000000
case class Metric(partitionDateKey: String, partitionTypeKey:String, backupCount:Option[Long], rawCount:Option[Long], diff:Option[Long])

def identifyMissingDataAndExtractDiffs(date: LocalDate, events: Seq[String]): Seq[Metric] = {
      println(date)

      val (y, m, d) = (date.getYear, date.getMonthValue, date.getDayOfMonth)
      val metrics = events.map { msg =>
        val curLccRawPath = s"$lccProdData/PartitionDateKey=$date/PartitionTypeKey=$msg"
        val curBackupFolder = f"/$msg/$y/$m%02d/$d%02d"
        val blobEx = blobFS.exists(new HadoopPath(curBackupFolder))
        val dlEx = dlFS.exists(new HadoopPath(curLccRawPath))

        if (blobEx) {
          val blobDF = spark
            .read.text(f"$lccBackupPath/$curBackupFolder/*")
            .withColumn("parsed", parseLCC($"value"))
            .filter($"parsed".isNotNull)
            .withColumn("key", getLCCKey($"parsed.macId", $"parsed.firstJSON"))

          val diff =
            if (dlEx) {
              val dlDF = spark
                .read.parquet(curLccRawPath)
                .select(getLCCKey($"MacId", $"MessageProperties").alias("key"))
              blobDF.join(dlDF, Seq("key"), "left_anti")
            } else {
              (blobDF)
            }

          val rawCount =
            if (dlEx) {
              val dlDF = spark
                .read.parquet(curLccRawPath)
                .select(getLCCKey($"MacId", $"MessageProperties").alias("key"))
              dlDF.count
            } else {
              0l
            }

          diff.cache()

          val diffCount = diff.count
          println(s"diff count: $diffCount")
          val blobBackupCount = blobDF.count
          println(s"blobBackupCount count: $blobBackupCount")
          val backupMinusRawCount = blobBackupCount - rawCount
          println(s"backupMinusRawCount count: $backupMinusRawCount")

          println(s" |$msg \n BACKUP count: $blobBackupCount \n RAW count: $rawCount \n (BACKUP - RAW): $backupMinusRawCount \n DIFF: $diffCount \n Are diffs the same? ${diffCount == backupMinusRawCount}")


        //Restore data
        if (diffCount != 0) {
          val partitionNumber = if(diffCount > recordsPerPartition) diffCount / recordsPerPartition else 1

          diff.select(getLCCConvertedDF($"parsed.macId", $"parsed.timestamp", $"parsed.firstJSON",
            $"parsed.secondJSON", lit(msg)).alias("data"))
            .select("data.*")
            .filter($"EventTime".isNotNull)
            .withColumn("PartitionDateKey", getPartitionDateKey($"EventTime"))
            .withColumn("PartitionTypeKey", lit(msg))
            .repartition(partitionNumber.toInt)
            .write
            .mode("append")
            .partitionBy("PartitionDateKey", "PartitionTypeKey")
            .parquet(restoreOutputPath)

          println(s"PartitionDateKey: $date PartitionTypeKey: $msg is restored")
        } else {
          println(s"PartitionDateKey: $date PartitionTypeKey: $msg is skipped")
        }
     diff.unpersist()

     Metric(date.toString, msg, Some(blobBackupCount), Some(rawCount), Some(diffCount))
      }
        else {
        println(s"!!!!!!!!! Backup data for $msg doesn't exist !!!!!!!!!")
          Metric(date.toString, msg, None, None, None)
      }
}
    metrics
}

// COMMAND ----------

// MAGIC %md
// MAGIC # 3 Move files to prod directory

// COMMAND ----------

def moveFiles(dlFS: org.apache.hadoop.fs.FileSystem, srcFolder: String, dstFolder: String, date: LocalDate) {
    val srcPath = new HadoopPath(s"$srcFolder/PartitionDateKey=$date")
    val dirs = dlFS.listStatus(srcPath).map(_.getPath)

    dirs.foreach { src =>
      println(s"Processing: $src")
      val dst = new HadoopPath(s"$dstFolder/PartitionDateKey=$date/${src.getName}")
      val dstEx = dlFS.exists(dst)
      if (!dstEx) {
        println(s"dlFS.mkdirs(dst): $dst")
        dlFS.mkdirs(dst)
      }
      val files = dlFS.listStatus(src).map(_.getPath).filterNot(file=> file.getName.startsWith("_"))
      files.foreach { file =>
        println(s"dlFS.rename($file, new HadoopPath($dst/rec3-${file.getName}")
        dlFS.rename(file, new HadoopPath(s"$dst/rec3-${file.getName}"))
      }
      //dlFS.delete(src, false)
      //println(s"dlFS.delete($src, false)")
    }

    //dlFS.delete(srcPath, false)
    //println(s"dlFS.delete($srcPath, false)")
  }

// COMMAND ----------

// MAGIC %md
// MAGIC # 4 Execute all steps

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4.1 Execute 2 combined steps

// COMMAND ----------

val metric = daysToCheck.flatMap(identifyMissingDataAndExtractDiffs(_, allMsgs))
val metricSeq = metric.toSeq
val metricDs = metricSeq.toDS()
metricDs
  .repartition(1)
  .write
  .mode("append")
  .parquet(metricsPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4.2 Execute '3 Move files to prod directory'

// COMMAND ----------

//Automatically
val missingDates = metricDs.select("partitionDateKey").distinct.as[String].collect

// COMMAND ----------

val datesToRecover = missingDates.map(LocalDate.parse(_, DateTimeFormatter.ofPattern("yyyy-MM-dd")))
datesToRecover.foreach(date => moveFiles(dlFS, diffPath, lccProdData, date))

//delete _tmp folder
dlFS.delete(new HadoopPath(diffPath), true)

// COMMAND ----------

val resultMessage = metric.map(m => s"date=${m.partitionDateKey}, type=${m.partitionTypeKey}, diff=${m.diff.getOrElse(0)}").mkString("|")

// COMMAND ----------

dbutils.notebook.exit(s"$resultMessage")
