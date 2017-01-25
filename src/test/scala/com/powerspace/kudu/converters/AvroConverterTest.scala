package com.powerspace.kudu.converters

import org.apache.kudu.Type._
import org.scalatest.{FlatSpec, Matchers}
import org.apache.avro.Schema.{Type => AvroType}
import org.apache.avro.SchemaParseException

class AvroConverterTest extends FlatSpec with Matchers {

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
    val converter = new AvroConverter(schema)
    val List(a, b, c) = converter.avroColumns()
    a.name should === ("valid")
    a.schema.getType should === (AvroType.BOOLEAN)
    b.name should === ("eventType")
    b.schema.getType should === (AvroType.STRING)
    c.name should === ("id")
    c.schema.getType should === (AvroType.LONG)
  }

  it should "properly map to kudu columns" in {
    val converter = new AvroConverter(schema)
    val List(a, b, c) = converter.kuduColumns()
    a.name should === ("valid")
    a.builder.build().getName should === ("valid")
    a.builder.build().getType should === (BOOL)

    b.name should === ("eventType")
    b.builder.build().getName should === ("eventType")
    b.builder.build().getType should === (STRING)

    c.name should === ("id")
    c.builder.build().getName should === ("id")
    c.builder.build().getType should === (INT64)
  }

  it should "crash if it found an unknown type" in {
    an [SchemaParseException] should be thrownBy  new AvroConverter(unknownTypeSchema)
  }

}
