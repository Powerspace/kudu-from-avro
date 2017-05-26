package com.powerspace.kudu

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.kudu.{ColumnSchema, Type}
import org.scalatest.FunSuite

import scala.collection.mutable

class AvroComparatorTest extends FunSuite {

  val previousSchema =
    """
      |{
      | "namespace": "com.powerspace.test",
      |	"name": "TestAvro",
      |	"type": "record",
      |	"fields" : [
      |		{"name":"valid","type":"boolean"},
      |		{"name":"eventType","type":"string"},
      |		{"name":"id","type":"long"}
      |  ]
      |}
    """.stripMargin

  val nextSchema =
    """
      |{
      | "namespace": "com.powerspace.test",
      |	"name": "TestAvro",
      |	"type": "record",
      |	"fields" : [
      |		{"name":"valid","type":"boolean"},
      |		{"name":"eventType","type":"string"},
      |		{"name":"id","type":"long"},
      |   {"name":"square","type":"int","default":1},
      |   {"name":"str","type":["null", "string"]}
      |  ]
      |}
    """.stripMargin


  test("test compare and add fields") {
    val fields: Seq[ColumnSchema] = AvroComparator.columnsToAdd(
      new Schema.Parser().parse(previousSchema),
      new Schema.Parser().parse(nextSchema)).map(_._2.build()).toList

    assert(fields.size == 2)

    assert(fields(0).getDefaultValue == 1)
    assert(fields(0).getName == "square")
    assert(fields(0).getType == Type.INT32)
    assert(fields(0).isNullable == false)

    assert(fields(1).getDefaultValue == null)
    assert(fields(1).getName == "str")
    assert(fields(1).getType == Type.STRING)
    assert(fields(1).isNullable == true)


    val toRemove: mutable.Seq[Field] = AvroComparator.toRemove(
      new Schema.Parser().parse(previousSchema),
      new Schema.Parser().parse(nextSchema))

    assert(toRemove.isEmpty)

  }

  test("test compare and remove fields") {
    val fields: Seq[ColumnSchema] = AvroComparator.columnsToAdd(
      new Schema.Parser().parse(nextSchema),
      new Schema.Parser().parse(previousSchema)).map(_._2.build()).toList

    assert(fields.isEmpty)

    val toRemove: mutable.Seq[Field] = AvroComparator.toRemove(
      new Schema.Parser().parse(nextSchema),
      new Schema.Parser().parse(previousSchema))

    assert(toRemove.size == 2)
  }


}
