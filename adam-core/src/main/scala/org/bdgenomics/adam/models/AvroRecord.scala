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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.specific.SpecificRecord

object AvroRecord {
  val NULL = Schema.create(Schema.Type.NULL)
  val STRING = Schema.create(Schema.Type.STRING)
  val BOOLEAN = Schema.create(Schema.Type.BOOLEAN)
  val INT = Schema.create(Schema.Type.INT)
  val LONG = Schema.create(Schema.Type.LONG)

  private val schemaCache = new mutable.HashMap[Class[_], Schema]()
}

trait AvroRecord[T] extends SpecificRecord {
  self: T =>

  def avroBinding(): List[(String, Schema, (T) => AnyRef, (T, Any) => Unit)]

  def utf8ToString(utf8: Any): String = Option(utf8).map(_.toString).orNull

  override def get(i: Int): AnyRef = avroBinding()(i)._3(self)

  override def put(i: Int, v: scala.Any): Unit = avroBinding()(i)._4(self, v)

  override def getSchema: Schema =
    AvroRecord.schemaCache.getOrElseUpdate(self.getClass, {
      require(self.getClass != classOf[Nothing], "Missing class type. Use AvroRecord[MyClass](...)")
      val className = self.getClass.getName.stripSuffix("$")
      val record = Schema.createRecord(className, null, null, false)
      record.setFields(avroBinding().map {
        case (fieldName: String, fieldSchema: Schema, _, _) =>
          new Field(fieldName, Schema.createUnion(List(AvroRecord.NULL, fieldSchema).asJava),
            null, null, Schema.Field.Order.IGNORE)
      }.asJava)
      record
    })
}
