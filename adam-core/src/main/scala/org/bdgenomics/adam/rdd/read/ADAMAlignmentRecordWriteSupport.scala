/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd.read

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroWriteSupport
import org.apache.parquet.hadoop.api.DelegatingWriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.{ WriteContext, FinalizedWriteContext }
import org.apache.parquet.io.api.RecordConsumer
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary, RecordGroupDictionary, RecordGroup}
import org.bdgenomics.formats.avro.AlignmentRecord
import org.json4s.jackson.Serialization.{read => jsonRead, write => jsonWrite, _}

private abstract class MetadataObject[T] {
  implicit val formats = org.json4s.DefaultFormats
  val uniqueId: String
  def serialize(t: T): String
  def deserialize(s: String): T
  def readFromConfiguration(conf: Configuration): Option[T] = {
    Option(conf.get(uniqueId)).map(deserialize)
  }
  def writeToConfiguration(conf: Configuration, t: T) = {
    Option(conf.get(uniqueId)).map { value =>
      throw new IllegalStateException(s"${uniqueId} is already set to ${value}")
    }
    conf.set(uniqueId, serialize(t))
  }
}

private object RecordGroupDictionaryMetadata extends MetadataObject[RecordGroupDictionary] {
  override val uniqueId: String = "adam.recordGroup"

  override def serialize(recordGroupDictionary: RecordGroupDictionary): String = {
    jsonWrite(recordGroupDictionary.recordGroups)
  }

  override def deserialize(s: String): RecordGroupDictionary = {
    val recordGroups = jsonRead[Seq[RecordGroup]](s)
    new RecordGroupDictionary(recordGroups)
  }
}

private object SequenceDictionaryMetadata extends MetadataObject[SequenceDictionary] {
  override val uniqueId: String = "adam.sequenceDictionary"

  override def serialize(t: SequenceDictionary): String = {
    jsonWrite(t.records)
  }

  override def deserialize(s: String): SequenceDictionary = {
    val records = jsonRead[Seq[SequenceRecord]](s)
    new SequenceDictionary(records.toVector)
  }
}

object ADAMAlignmentRecordWriteSupport {

  def setRecordGroupDictionary(conf: Configuration, recordGroupDictionary: RecordGroupDictionary) = {
    RecordGroupDictionaryMetadata.writeToConfiguration(conf, recordGroupDictionary)
  }

  def getRecordGroupDictionary(conf: Configuration): Option[RecordGroupDictionary] = {
    RecordGroupDictionaryMetadata.readFromConfiguration(conf)
  }

  def setSequenceDictionary(conf: Configuration, seqDict: SequenceDictionary) = {
    SequenceDictionaryMetadata.writeToConfiguration(conf, seqDict)
  }

  def getSequenceDictionary(conf: Configuration): Option[SequenceDictionary] = {
    SequenceDictionaryMetadata.readFromConfiguration(conf)
  }
}

/*
case class ReferenceMetrics(referenceName: String, minPos: Long, maxPos: Long)

class ReferenceMetricBuilder(referenceName: String) {
  var minPos = -1L
  var maxPos = -1L

  def updateMetrics(record: AlignmentRecord): Unit = {
    Option(record.getStart).foreach(start => if (start < minPos) minPos = start)
    Option(record.getEnd).foreach(end => if (end > maxPos) maxPos = end)
  }

  def build(): ReferenceMetrics = {
    ReferenceMetrics(referenceName, minPos, maxPos)
  }
}*/

class ADAMAlignmentRecordWriteSupport extends DelegatingWriteSupport[AlignmentRecord](new AvroWriteSupport) {
  import ADAMAlignmentRecordWriteSupport._
  var recordGroups: Option[RecordGroupDictionary] = None
  var sequenceDictionary: Option[SequenceDictionary] = None
  //var referenceMetrics = new mutable.HashMap[String, ReferenceMetrics]()

  override def finalizeWrite(): FinalizedWriteContext = {
    var metaData: mutable.Map[String, String] = new mutable.HashMap[String, String]()
    if (recordGroups.isDefined) {
      metaData += (RecordGroupDictionaryMetadata.uniqueId ->
        RecordGroupDictionaryMetadata.serialize(recordGroups.get))
    }
    if (sequenceDictionary.isDefined) {
      metaData += (SequenceDictionaryMetadata.uniqueId ->
        SequenceDictionaryMetadata.serialize(sequenceDictionary.get))
    }
    new FinalizedWriteContext(metaData.asJava)
  }

  override def init(configuration: Configuration): WriteContext = {
    val writeContext = super.init(configuration)
    recordGroups = getRecordGroupDictionary(configuration)
    sequenceDictionary = getSequenceDictionary(configuration)
    writeContext
  }

  override def write(record: AlignmentRecord): Unit = {
    super.write(record)
  }

  override def toString: String = super.toString

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = super.prepareForWrite(recordConsumer)
}
