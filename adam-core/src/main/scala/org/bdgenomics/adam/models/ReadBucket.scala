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

import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * This class is similar to SingleReadBucket, except it breaks the reads down further.
 *
 * Rather than stopping at primary/secondary/unmapped, this will break it down further into whether they are paired
 * or unpaired, and then whether they are the first or second of the pair.
 *
 * This is useful as this will usually map a single read in any of the sequences.
 */
case class ReadBucket(unpairedPrimaryMappedReads: Iterable[AlignmentRecord] = Seq.empty,
                      pairedFirstPrimaryMappedReads: Iterable[AlignmentRecord] = Seq.empty,
                      pairedSecondPrimaryMappedReads: Iterable[AlignmentRecord] = Seq.empty,
                      unpairedSecondaryMappedReads: Iterable[AlignmentRecord] = Seq.empty,
                      pairedFirstSecondaryMappedReads: Iterable[AlignmentRecord] = Seq.empty,
                      pairedSecondSecondaryMappedReads: Iterable[AlignmentRecord] = Seq.empty,
                      unmappedReads: Iterable[AlignmentRecord] = Seq.empty) {
  def allReads(): Iterable[AlignmentRecord] =
    unpairedPrimaryMappedReads ++
      pairedFirstPrimaryMappedReads ++
      pairedSecondPrimaryMappedReads ++
      unpairedSecondaryMappedReads ++
      pairedFirstSecondaryMappedReads ++
      pairedSecondSecondaryMappedReads ++
      unmappedReads
}

object ReadBucket {
  implicit def singleReadBucketToReadBucket(bucket: SingleReadBucket): ReadBucket = {
    // check that reads are either first or second read from fragment
    bucket.primaryMapped.foreach(r => require(r.getReadNum >= 0 && r.getReadNum <= 1,
      "Read %s is not first or second read from pair (num = %d).".format(r, r.getReadNum)))
    bucket.secondaryMapped.foreach(r => require(r.getReadNum >= 0 && r.getReadNum <= 1,
      "Read %s is not first or second read from pair (num = %d).".format(r, r.getReadNum)))
    bucket.unmapped.foreach(r => require(r.getReadNum >= 0 && r.getReadNum <= 1,
      "Read %s is not first or second read from pair (num = %d).".format(r, r.getReadNum)))

    val (pairedPrimary, unpairedPrimary) = bucket.primaryMapped.partition(_.getReadPaired)
    val (pairedFirstPrimary, pairedSecondPrimary) = pairedPrimary.partition(_.getReadNum == 0)
    val (pairedSecondary, unpairedSecondary) = bucket.secondaryMapped.partition(_.getReadPaired)
    val (pairedFirstSecondary, pairedSecondSecondary) = pairedSecondary.partition(_.getReadNum == 0)

    new ReadBucket(unpairedPrimary,
      pairedFirstPrimary,
      pairedSecondPrimary,
      unpairedSecondary,
      pairedFirstSecondary,
      pairedSecondSecondary,
      bucket.unmapped)
  }
}
