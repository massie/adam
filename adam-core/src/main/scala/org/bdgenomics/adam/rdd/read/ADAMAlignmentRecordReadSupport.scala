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

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.api.DelegatingReadSupport
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api.{ GroupConverter, RecordMaterializer }
import org.apache.parquet.schema.MessageType
import org.bdgenomics.adam.models.{SequenceDictionary, RecordGroupDictionary}
import org.bdgenomics.formats.avro.AlignmentRecord

object ADAMAlignmentRecordReadSupport {

  def getRecordGroupDictionary(job: Job): Option[RecordGroupDictionary] = {
    RecordGroupDictionaryMetadata.readFromConfiguration(job)
  }

  def getSequenceDictionary(job: Job): Option[SequenceDictionary] = {
    SequenceDictionaryMetadata.readFromConfiguration(job)
  }

}

class ADAMAlignmentRecordReadSupport extends DelegatingReadSupport[AlignmentRecord](new AvroReadSupport()) {

  override def prepareForRead(configuration: Configuration,
                              keyValueMetaData: util.Map[String, String],
                              fileSchema: MessageType,
                              readContext: ReadContext): RecordMaterializer[AlignmentRecord] = {
    val avroReader = super.prepareForRead(configuration, keyValueMetaData, fileSchema, readContext)
    val recordGroups = RecordGroupDictionaryMetadata.readFromConfiguration(configuration)

    new RecordMaterializer[AlignmentRecord] {
      override def getRootConverter: GroupConverter = avroReader.getRootConverter

      override def getCurrentRecord: AlignmentRecord = {
        avroReader.getCurrentRecord
        /*
        recordGroups match {
          case Some(rg) =>
            // If we have metadata, merge it with the record
            rg.get((record.getRecordGroupSample, record.getRecordGroupName)) match {
              case Some(metadata) =>
                val builder = AlignmentRecord.newBuilder(record)
                  .setRecordGroupDescription(metadata.description.orNull)
                  .setRecordGroupFlowOrder(metadata.flowOrder.orNull)
                  .setRecordGroupKeySequence(metadata.keySequence.orNull)
                  .setRecordGroupLibrary(metadata.library.orNull)
                  .setRecordGroupName(metadata.recordGroupName)
                  .setRecordGroupPlatform(metadata.platform.orNull)
                  .setRecordGroupPlatformUnit(metadata.platformUnit.orNull)
                  .setRecordGroupSample(metadata.sample)
                  .setRecordGroupSequencingCenter(metadata.sequencingCenter.orNull)
                metadata.predictedMedianInsertSize.map(i => builder.setRecordGroupPredictedMedianInsertSize(i))
                metadata.runDateEpoch.map(l => builder.setRecordGroupRunDateEpoch(l))
                builder.build()
              case None =>
                // We have no metadata for this record in our metadata field, return as is
                // TODO: Is this correct behavior? Should we fail here?
                record
            }

          case None =>
            // If there are no record groups in the metadata, just return it as is
            record
        }*/
      }
    }
  }
}
