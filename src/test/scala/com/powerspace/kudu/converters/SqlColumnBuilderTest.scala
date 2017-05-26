package com.powerspace.kudu.converters

import org.apache.kudu.ColumnSchema.CompressionAlgorithm
import org.apache.kudu.ColumnSchema.CompressionAlgorithm.LZ4
import org.apache.kudu.Type._
import org.scalatest.{FlatSpec, Matchers}

class SqlColumnBuilderTest extends FlatSpec with Matchers {

  "the sql converter" should "properly parse sql" in {
    val converter = new SqlColumnBuilder(" name STRING,      id INT", List())
    converter.sqlColumns() should contain only(
      SqlColumn("name", "STRING"),
      SqlColumn("id", "INT"))
  }

  it should "properly map to kudu columns" in {
    val converter = new SqlColumnBuilder(" name STRING,      id BIGINT", List())
    val List(a, b) = converter.kuduColumns(LZ4, List())
   // a.name should === ("name")
    a.getName should === ("name")
    a.getType should === (STRING)

   // b.name should === ("id")
    b.getName should === ("id")
    b.getType should === (INT64)
  }


  it should "crash if it found an unknown type" in {
    val converter = new SqlColumnBuilder(" name STRING,     unknown  JOHN", List())
    an [IllegalArgumentException] should be thrownBy converter.kuduColumns(LZ4, List())
  }
}
