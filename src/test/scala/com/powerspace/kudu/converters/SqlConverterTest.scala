package com.powerspace.kudu.converters

import org.apache.kudu.Type._
import org.scalatest.{FlatSpec, Matchers}

class SqlConverterTest extends FlatSpec with Matchers {

  "the sql converter" should "properly parse sql" in {
    val converter = new SqlConverter(" name STRING,      id INT")
    converter.sqlColumns() should contain only(
      SqlColumn("name", "STRING"),
      SqlColumn("id", "INT"))
  }

  it should "properly map to kudu columns" in {
    val converter = new SqlConverter(" name STRING,      id BIGINT")
    val List(a, b) = converter.kuduColumns()
    a.name should === ("name")
    a.builder.build().getName should === ("name")
    a.builder.build().getType should === (STRING)

    b.name should === ("id")
    b.builder.build().getName should === ("id")
    b.builder.build().getType should === (INT64)
  }


  it should "crash if it found an unknown type" in {
    val converter = new SqlConverter(" name STRING,     unknown  JOHN")
    an [IllegalArgumentException] should be thrownBy converter.kuduColumns()
  }
}
