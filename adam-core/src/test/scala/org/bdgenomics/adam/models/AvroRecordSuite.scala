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

import it.unimi.dsi.fastutil.io.{ FastByteArrayInputStream, FastByteArrayOutputStream }
import org.apache.avro.io.{ BinaryDecoder, DecoderFactory, BinaryEncoder, EncoderFactory }
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter }
import org.apache.avro.util.Utf8
import org.scalatest.FunSuite

case class NonAvroCaseClass(var name: String, var value: Long) extends AvroRecord[NonAvroCaseClass] {
  def this() = this(null, -1) // For avro
  override def avroBinding() =
    List(("name", AvroRecord.STRING,
      (t: NonAvroCaseClass) => t.name,
      (t: NonAvroCaseClass, v: Any) => { t.name = Option(v.asInstanceOf[Utf8]).map(_.toString).orNull }),
      ("value", AvroRecord.LONG,
        (t: NonAvroCaseClass) => t.value.asInstanceOf[AnyRef],
        (t: NonAvroCaseClass, v: Any) => { t.value = v.asInstanceOf[Long] }))
}

class AvroRecordSuite extends FunSuite {

  test("AvroRecords can be serialized/deserialized") {
    val out = new FastByteArrayOutputStream()
    val encoder = EncoderFactory.get().directBinaryEncoder(out, null.asInstanceOf[BinaryEncoder])
    val writer = new SpecificDatumWriter[NonAvroCaseClass](new NonAvroCaseClass().getSchema)

    val inRecord = NonAvroCaseClass("This is my name", 42)
    writer.write(inRecord, encoder)
    encoder.flush()

    val bytes = out.array
    val in = new FastByteArrayInputStream(bytes)
    val decoder = DecoderFactory.get().directBinaryDecoder(in, null.asInstanceOf[BinaryDecoder])
    val reader = new SpecificDatumReader[NonAvroCaseClass](new NonAvroCaseClass().getSchema)

    val outRecord = reader.read(null.asInstanceOf[NonAvroCaseClass], decoder)

    assert(inRecord == outRecord)
    in.close()
    out.close()
  }

}
