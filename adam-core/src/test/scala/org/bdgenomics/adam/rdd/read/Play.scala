package org.bdgenomics.adam.rdd.read

import java.io.FileNotFoundException
import java.util.logging.Level

import htsjdk.samtools.SAMFileHeader
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport, AvroParquetInputFormat, AvroReadSupport}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.ContextUtil
import org.bdgenomics.adam.converters.SAMRecordConverter
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary}
import org.bdgenomics.adam.rdd.InstrumentedADAMAvroParquetOutputFormat
import org.bdgenomics.adam.util.ParquetLogger
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.misc.HadoopUtil
import org.seqdoop.hadoop_bam.{SAMRecordWritable, AnySAMInputFormat}
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

import org.apache.spark.{SparkConf, Logging, SparkContext}
import org.apache.spark.rdd.RDD

object Play extends Logging {
  val conf = new SparkConf()
  conf.setAppName("test")
  conf.setMaster("local[4]")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
  val sc = new SparkContext(conf)

  def loadBam(filePath: String): (RDD[AlignmentRecord], SequenceDictionary, RecordGroupDictionary) = {
    val path = new Path(filePath)
    val fs =
      Option(
        FileSystem.get(path.toUri, sc.hadoopConfiguration)
      ).getOrElse(
        throw new FileNotFoundException(
          s"Couldn't find filesystem for ${path.toUri} with Hadoop configuration ${sc.hadoopConfiguration}"
        )
      )

    val bamFiles =
      Option(
        if (fs.isDirectory(path)) fs.listStatus(path) else fs.globStatus(path)
      ).getOrElse(
        throw new FileNotFoundException(
          s"Couldn't find any files matching ${path.toUri}"
        )
      )

    val (seqDict, readGroups) =
      bamFiles
        .map(fs => fs.getPath)
        .flatMap(fp => {
          try {
            // We need to separately read the header, so that we can inject the sequence dictionary
            // data into each individual Read (see the argument to samRecordConverter.convert,
            // below).
            val samHeader = SAMHeaderReader.readSAMHeaderFrom(fp, sc.hadoopConfiguration)
            log.info("Loaded header from " + fp)
            val sd = SequenceDictionary(samHeader)
            val rg = RecordGroupDictionary.fromSAMHeader(samHeader)
            Some((sd, rg))
          } catch {
            case e: Throwable => {
              log.error(
                s"Loading failed for $fp:n${e.getMessage}\n\t${e.getStackTrace.take(25).map(_.toString).mkString("\n\t")}"
              )
              None
            }
          }
        }).reduce((kv1, kv2) => {
        (kv1._1 ++ kv2._1, kv1._2 ++ kv2._2)
      })

    val job = HadoopUtil.newJob(sc)
    val records = sc.newAPIHadoopFile(filePath, classOf[AnySAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], ContextUtil.getConfiguration(job))
    val samRecordConverter = new SAMRecordConverter
    (records.map(p => samRecordConverter.convert(p._2.get, seqDict, readGroups)), seqDict, readGroups)
  }

  def adamParquetSave(rdd: RDD[AlignmentRecord],
                      seqDict: SequenceDictionary,
                      recordsGroups: RecordGroupDictionary,
                      filePath: String,
                      blockSize: Int = 128 * 1024 * 1024,
                      pageSize: Int = 1 * 1024 * 1024,
                      compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
                      disableDictionaryEncoding: Boolean = false,
                      schema: Option[Schema] = None): Unit = SaveAsADAM.time {
    log.info("Saving data in ADAM format")

    val job = HadoopUtil.newJob(rdd.context)
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[ADAMAlignmentRecordWriteSupport])
    ADAMAlignmentRecordWriteSupport.setRecordGroupDictionary(job, recordsGroups)
    ADAMAlignmentRecordWriteSupport.setSequenceDictionary(job, seqDict)
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(job,
      if (schema.isDefined) schema.get else AlignmentRecord.SCHEMA$)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], classOf[AlignmentRecord],
      classOf[InstrumentedADAMAvroParquetOutputFormat],
      ContextUtil.getConfiguration(job))
  }

  def loadParquet[T](filePath: String,
                     predicate: Option[FilterPredicate] = None,
                     projection: Option[Schema] = None)(implicit ev1: T => SpecificRecord, ev2: Manifest[T]):
  (RDD[T], Option[SequenceDictionary], Option[RecordGroupDictionary]) = {
    //make sure a type was specified
    //not using require as to make the message clearer
    if (manifest[T] == manifest[scala.Nothing])
      throw new IllegalArgumentException("Type inference failed; when loading please specify a specific type. " +
        "e.g.:\nval reads: RDD[AlignmentRecord] = ...\nbut not\nval reads = ...\nwithout a return type")

    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[ADAMAlignmentRecordReadSupport])

    if (predicate.isDefined) {
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, predicate.get)
    }

    if (projection.isDefined) {
      AvroParquetInputFormat.setRequestedProjection(job, projection.get)
    }

    (sc.newAPIHadoopFile(
      filePath,
      classOf[ParquetInputFormat[T]],
      classOf[Void],
      manifest[T].runtimeClass.asInstanceOf[Class[T]],
      ContextUtil.getConfiguration(job)
    ).map(p => p._2).filter(p => p != null.asInstanceOf[T]),
      ADAMAlignmentRecordReadSupport.getSequenceDictionary(job),
      ADAMAlignmentRecordReadSupport.getRecordGroupDictionary(job))
  }

  def main(args: Array[String]): Unit = {
    val testFile = "/workspace/data/exampleBAM.bam"

    val (records, seqDict, recordGroups) = loadBam(testFile)
    records.foreach(println)
    println(seqDict)
    println(recordGroups)

    val testOut = "/tmp/output"
    adamParquetSave(records.coalesce(2, shuffle= true), seqDict, recordGroups, testOut)

    val (outRecords, outSeqDict, outRecordGroups) = loadParquet[AlignmentRecord](testOut)
    assert(seqDict == outSeqDict.get)
    assert(recordGroups == outRecordGroups.get)
  }

}
