package com.powerspace.kudu.converters

import org.apache.kudu.Type._
import org.scalatest.{FlatSpec, Matchers}
import org.apache.avro.Schema.{Type => AvroType}
import org.apache.avro.SchemaParseException
import org.apache.kudu.ColumnSchema.CompressionAlgorithm.LZ4

class AvroColumnBuilderTest extends FlatSpec with Matchers {

  val schema =
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


  val unknownTypeSchema =
    """
      |{
      | "namespace": "com.powerspace.test",
      |	"name": "TestAvro",
      |	"type": "record",
      |	"fields" : [
      |		{"name":"valid","type":"boolean"},
      |		{"name":"eventType","type":"string"},
      |		{"name":"id","type":"foobar"}
      |  ]
      |}
    """.stripMargin

  "the Avro converter" should "properly parse an Avro schema" in {
    val converter = AvroColumnBuilder(schema)
    val List(a, b, c) = converter.fields()
    a.name should === ("valid")
    a.schema.getType should === (AvroType.BOOLEAN)
    b.name should === ("eventType")
    b.schema.getType should === (AvroType.STRING)
    c.name should === ("id")
    c.schema.getType should === (AvroType.LONG)
  }

  it should "properly map to kudu columns" in {
    val converter = AvroColumnBuilder(schema)
    val List(a, b, c) = converter.kuduColumns(LZ4, List())
    a.getName should === ("valid")
    a.getType should === (BOOL)

    b.getName should === ("eventType")
    b.getType should === (STRING)

    c.getName should === ("id")
    c.getType should === (INT64)
  }

  it should "crash if it found an unknown type" in {
    an [SchemaParseException] should be thrownBy AvroColumnBuilder(unknownTypeSchema)
  }

}
