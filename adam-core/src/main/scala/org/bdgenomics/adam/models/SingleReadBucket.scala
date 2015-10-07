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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  Fragment
}
import scala.collection.JavaConverters._

object SingleReadBucket extends Logging {
  def apply(rdd: RDD[AlignmentRecord]): RDD[SingleReadBucket] = {
    rdd.groupBy(p => (p.getRecordGroupName, p.getReadName))
      .map(kv => {
        val (_, reads) = kv

        // split by mapping
        val (mapped, unmapped) = reads.partition(_.getReadMapped)
        val (primaryMapped, secondaryMapped) = mapped.partition(_.getPrimaryAlignment)

        // TODO: consider doing validation here (e.g. read says mate mapped but it doesn't exist)
        new SingleReadBucket(primaryMapped, secondaryMapped, unmapped)
      })
  }
}

case class SingleReadBucket(var primaryMapped: Iterable[AlignmentRecord] = Seq.empty,
                            var secondaryMapped: Iterable[AlignmentRecord] = Seq.empty,
                            var unmapped: Iterable[AlignmentRecord] = Seq.empty) extends AvroRecord[SingleReadBucket] {

  def this() = this(Seq.empty, Seq.empty, Seq.empty) // For Avro...

  def avroBinding() = {
    List(
      ("primaryMapped", Schema.createArray(AlignmentRecord.SCHEMA$),
        (a: SingleReadBucket) => a.primaryMapped.asJava,
        (a: SingleReadBucket, v: Any) => {
          a.primaryMapped =
            v.asInstanceOf[GenericData.Array[AlignmentRecord]].asScala
        }),
      ("secondaryMapped", Schema.createArray(AlignmentRecord.SCHEMA$),
        (a: SingleReadBucket) => a.secondaryMapped.asJava,
        (a: SingleReadBucket, v: Any) => {
          a.secondaryMapped =
            v.asInstanceOf[GenericData.Array[AlignmentRecord]].asScala
        }),
      ("unmapped", Schema.createArray(AlignmentRecord.SCHEMA$),
        (a: SingleReadBucket) => a.unmapped.asJava,
        (a: SingleReadBucket, v: Any) => {
          a.unmapped =
            v.asInstanceOf[GenericData.Array[AlignmentRecord]].asScala
        })
    )
  }

  // Note: not a val in order to save serialization/memory cost
  def allReads = {
    primaryMapped ++ secondaryMapped ++ unmapped
  }

  def toFragment: Fragment = {
    // take union of all reads, as we will need this for building and
    // want to pay the cost exactly once
    val unionReads = allReads

    // start building fragment
    val builder = Fragment.newBuilder()
      .setReadName(unionReads.head.getReadName)
      .setAlignments(allReads.toList.asJava)

    // is an insert size defined for this fragment?
    primaryMapped.headOption
      .foreach(r => {
        Option(r.getInferredInsertSize).foreach(is => {
          builder.setFragmentSize(is.toInt)
        })
      })

    // set platform unit, if known
    Option(unionReads.head.getRecordGroupPlatformUnit)
      .foreach(p => builder.setInstrument(p))

    // set record group name, if known
    Option(unionReads.head.getRecordGroupName)
      .foreach(n => builder.setRunId(n))

    builder.build()
  }
}

