/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.serialization

import java.io.{ InputStream, OutputStream }
import java.nio.ByteBuffer

import scala.collection.mutable

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer => KSerializer }
import it.unimi.dsi.fastutil.io.{ FastByteArrayInputStream, FastByteArrayOutputStream }
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io._
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter }
import org.apache.avro.util.Utf8
import org.bdgenomics.adam.models.{ ReferencePositionPair, ReferenceRegion, SingleReadBucket }
import org.bdgenomics.adam.rdd.read.realignment._
import org.bdgenomics.adam.util.{ TwoBitFile, TwoBitFileSerializer }
import org.bdgenomics.formats.avro._

import org.apache.spark.serializer.KryoRegistrator

class ADAMKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.setAutoReset(false)

    kryo.register(classOf[IndelRealignmentTarget])
    kryo.register(classOf[TargetSet], new TargetSetSerializer)
    kryo.register(classOf[ZippedTargetSet], new ZippedTargetSetSerializer)
    kryo.register(classOf[TwoBitFile], new TwoBitFileSerializer)

    val avroObjs = Seq[Class[_ <: IndexedRecord]](classOf[AlignmentRecord], classOf[Genotype],
      classOf[Variant], classOf[DatabaseVariantAnnotation],
      classOf[NucleotideContigFragment], classOf[Contig], classOf[StructuralVariant], classOf[VariantCallingAnnotations],
      classOf[VariantEffect], classOf[DatabaseVariantAnnotation], classOf[Dbxref], classOf[Feature],
      classOf[ReferencePositionPair], classOf[ReferenceRegion], classOf[SingleReadBucket])

    for (obj <- avroObjs) {
      kryo.register(obj, new ADAMKryoSerializer(obj.newInstance().getSchema))
    }
  }
}

private class ADAMKryoSerializer[T <: IndexedRecord](schema: Schema) extends KSerializer[T] {
  val reader = new SpecificDatumReader[T](schema)
  val writer = new SpecificDatumWriter[T](schema)
  val decoder = new ADAMAvroDecoder()
  val encoder = new ADAMAvroEncoder()

  override def write(kryo: Kryo, output: Output, t: T): Unit = {
    encoder.setOutputStream(output)
    writer.write(t, encoder)
  }

  override def read(kryo: Kryo, input: Input, klass: Class[T]): T = {
    decoder.setInputStream(input)
    reader.read(null.asInstanceOf[T], decoder)
  }
}

private object StringMessageType extends Enumeration {
  type StringMessageType = Value
  val NEW_STRING, EXISTING_STRING, EXPIRED_STRING = Value
}

private class ADAMAvroDecoder extends Decoder {
  var wrappedDecoder: Decoder = _
  var inputStream: Option[InputStream] = None

  def setInputStream(newInputStream: InputStream) = {
    if (!inputStream.exists(_ == newInputStream)) {
      // Update the I/O, if it changes
      inputStream = Some(newInputStream)
      wrappedDecoder = DecoderFactory.get().directBinaryDecoder(newInputStream, null.asInstanceOf[BinaryDecoder])
    }
  }

  private val scratchUtf8: Utf8 = new Utf8

  override def readLong(): Long = wrappedDecoder.readLong()

  override def readFloat(): Float = wrappedDecoder.readFloat()

  override def readNull(): Unit = wrappedDecoder.readNull()

  override def skipArray(): Long = wrappedDecoder.skipArray()

  override def skipMap(): Long = wrappedDecoder.skipMap()

  override def arrayNext(): Long = wrappedDecoder.arrayNext()

  override def mapNext(): Long = wrappedDecoder.mapNext()

  override def readFixed(bytes: Array[Byte], start: Int, length: Int): Unit =
    wrappedDecoder.readFixed(bytes, start, length)

  override def readEnum(): Int = wrappedDecoder.readEnum()

  override def readMapStart(): Long = wrappedDecoder.readMapStart()

  override def readInt(): Int = wrappedDecoder.readInt()

  override def readBytes(old: ByteBuffer): ByteBuffer = wrappedDecoder.readBytes(old)

  override def skipFixed(length: Int): Unit = wrappedDecoder.skipFixed(length)

  override def readArrayStart(): Long = wrappedDecoder.readArrayStart()

  override def skipBytes(): Unit = wrappedDecoder.skipBytes()

  override def readString(old: Utf8): Utf8 = {
    wrappedDecoder.readString(old)
  }

  override def readString(): String = wrappedDecoder.readString()

  override def readBoolean(): Boolean = wrappedDecoder.readBoolean()

  override def readIndex(): Int = wrappedDecoder.readIndex()

  override def skipString(): Unit = wrappedDecoder.skipString()

  override def readDouble(): Double = wrappedDecoder.readDouble()
}

private class ADAMAvroEncoder extends Encoder {
  var wrappedEncoder: Encoder = _
  var outputStream: Option[OutputStream] = None

  def setOutputStream(newOutputStream: OutputStream) = {
    if (!outputStream.exists(_ == newOutputStream)) {
      // Update the I/O, if it changes
      outputStream = Some(newOutputStream)
      wrappedEncoder = EncoderFactory.get().directBinaryEncoder(newOutputStream, null.asInstanceOf[BinaryEncoder])
    }
  }

  override def writeMapEnd(): Unit = wrappedEncoder.writeMapEnd()

  override def writeString(utf8: Utf8): Unit = wrappedEncoder.writeString(utf8)

  override def writeFloat(f: Float): Unit = wrappedEncoder.writeFloat(f)

  override def writeEnum(e: Int): Unit = wrappedEncoder.writeEnum(e)

  override def writeDouble(d: Double): Unit = wrappedEncoder.writeDouble(d)

  override def writeArrayEnd(): Unit = wrappedEncoder.writeArrayEnd()

  override def writeFixed(bytes: Array[Byte], start: Int, len: Int): Unit =
    wrappedEncoder.writeFixed(bytes, start, len)

  override def writeInt(n: Int): Unit = wrappedEncoder.writeInt(n)

  override def writeArrayStart(): Unit = wrappedEncoder.writeArrayStart()

  override def writeBoolean(b: Boolean): Unit = wrappedEncoder.writeBoolean(b)

  override def writeBytes(bytes: ByteBuffer): Unit = wrappedEncoder.writeBytes(bytes)

  override def writeBytes(bytes: Array[Byte], start: Int, len: Int): Unit =
    wrappedEncoder.writeBytes(bytes, start, len)

  override def writeMapStart(): Unit = wrappedEncoder.writeMapStart()

  override def writeLong(n: Long): Unit = wrappedEncoder.writeLong(n)

  override def writeNull(): Unit = wrappedEncoder.writeNull()

  override def setItemCount(itemCount: Long): Unit = wrappedEncoder.setItemCount(itemCount)

  override def writeIndex(unionIndex: Int): Unit = wrappedEncoder.writeIndex(unionIndex)

  override def startItem(): Unit = wrappedEncoder.startItem()

  override def flush(): Unit = wrappedEncoder.flush()
}

