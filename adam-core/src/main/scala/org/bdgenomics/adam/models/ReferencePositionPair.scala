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
package org.bdgenomics.adam.models

import org.apache.spark.Logging
import org.bdgenomics.adam.instrumentation.Timers.CreateReferencePositionPair
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord

object ReferencePositionPair extends Logging {
  def apply(singleReadBucket: SingleReadBucket): ReferencePositionPair = CreateReferencePositionPair.time {
    val firstOfPair = (singleReadBucket.primaryMapped.filter(_.getReadNum == 0) ++
      singleReadBucket.unmapped.filter(_.getReadNum == 0)).toSeq
    val secondOfPair = (singleReadBucket.primaryMapped.filter(_.getReadNum == 1) ++
      singleReadBucket.unmapped.filter(_.getReadNum == 1)).toSeq

    def getPos(r: AlignmentRecord): ReferencePosition = {
      if (r.getReadMapped) {
        new RichAlignmentRecord(r).fivePrimeReferencePosition
      } else {
        ReferencePosition(r.getSequence, 0L)
      }
    }

    if (firstOfPair.size + secondOfPair.size > 0) {
      new ReferencePositionPair(firstOfPair.lift(0).map(getPos),
        secondOfPair.lift(0).map(getPos))
    } else {
      new ReferencePositionPair((singleReadBucket.primaryMapped ++
        singleReadBucket.unmapped).toSeq.lift(0).map(getPos),
        None)
    }
  }
}

case class ReferencePositionPair(var read1refPos: Option[ReferencePosition],
                                 var read2refPos: Option[ReferencePosition]) extends AvroRecord[ReferencePositionPair] {
  def this() = this(None, None) // For Avro
  override def avroBinding() = {
    val referencePositionSchema = new ReferencePosition().getSchema
    List(
      ("read1refPos", referencePositionSchema,
        (p: ReferencePositionPair) => p.read1refPos.orNull,
        (p: ReferencePositionPair, v: Any) => { p.read1refPos = Option(v.asInstanceOf[ReferencePosition]) }),
      ("read2refPos", referencePositionSchema,
        (p: ReferencePositionPair) => p.read2refPos.orNull,
        (p: ReferencePositionPair, v: Any) => { p.read2refPos = Option(v.asInstanceOf[ReferencePosition]) }))
  }
}

