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

import org.apache.parquet.avro.AvroWriteSupport
import org.apache.parquet.hadoop.api.DelegatingWriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext
import org.bdgenomics.adam.models.RecordGroup
import org.bdgenomics.formats.avro.AlignmentRecord
import org.json4s.jackson.Serialization.{ write => jsonWrite }

object ADAMAlignmentRecordWriteSupport {
  implicit val formats = org.json4s.DefaultFormats
  val recordGroupMetadata = "adam.recordGroup.metadata"
}

class ADAMAlignmentRecordWriteSupport extends DelegatingWriteSupport[AlignmentRecord](new AvroWriteSupport) {
  import ADAMAlignmentRecordWriteSupport._
  var recordGroups = Set[RecordGroup]()

  override def finalizeWrite(): FinalizedWriteContext = {
    new FinalizedWriteContext(Map[String, String](recordGroupMetadata -> jsonWrite(recordGroups)).asJava)
  }

  override def write(record: AlignmentRecord): Unit = {
    RecordGroup(record) match {
      case Some(rg) =>
        // Save the record group info to the metadata
        recordGroups += rg
        // Scrub the record of recordgroup fields before writing it
        // Note: we keep the 'name' and 'sample' since they are lookup keys on the read path
        super.write(AlignmentRecord.newBuilder(record)
          .clearRecordGroupDescription()
          .clearRecordGroupFlowOrder()
          .clearRecordGroupKeySequence()
          .clearRecordGroupLibrary()
          .clearRecordGroupPlatform()
          .clearRecordGroupPlatformUnit()
          .clearRecordGroupPredictedMedianInsertSize()
          .clearRecordGroupRunDateEpoch()
          .clearRecordGroupSequencingCenter()
          .build())
      case None =>
        // If there's no RecordGroup information, just write the record as is
        super.write(record)
    }
  }
}
