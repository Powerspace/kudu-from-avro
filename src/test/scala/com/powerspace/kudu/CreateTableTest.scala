package com.powerspace.kudu

import com.powerspace.kudu.cli.CreateTableCliParser
import com.powerspace.kudu.converters.{Converter, KuduColumnBuilder}
import org.apache.kudu.ColumnSchema.{ColumnSchemaBuilder, CompressionAlgorithm}
import org.apache.kudu.Type
import org.scalatest.{FlatSpec, Matchers}

import scala.language.implicitConversions

class CreateTableTest extends FlatSpec with Matchers {

  implicit def strToKey(s: String): HashedKey = HashedKey(s)

  it should "parse the command line properly" in {
    val conf = CreateTableCliParser.parse("-t test -k server -p id:44,name:8,unknown".split(' ')).get
    conf.pkeys(0) shouldBe HashedKey("id", 44)
    conf.pkeys(1) shouldBe HashedKey("name", 8)
    conf.pkeys(2) shouldBe HashedKey("unknown")
  }

  it should "build Kudu columns properly" in {
    val converter = new Converter {
      override def kuduColumns(): List[KuduColumnBuilder] = {
        List(
          KuduColumnBuilder("name", new ColumnSchemaBuilder("name", Type.STRING)),
          KuduColumnBuilder("active", new ColumnSchemaBuilder("active", Type.BOOL)),
          KuduColumnBuilder("id", new ColumnSchemaBuilder("id", Type.INT64))
        )
      }
    }

    val List(a, b, c) = CreateTable.buildKuduColumns(converter, List("id"), compressed = true)

    a.getName should === ("id")
    a.getType should === (Type.INT64)
    a.getCompressionAlgorithm should === (CompressionAlgorithm.LZ4)

    b.getName should === ("name")
    b.getType should === (Type.STRING)
    b.getCompressionAlgorithm should === (CompressionAlgorithm.LZ4)

    c.getName should === ("active")
    c.getType should === (Type.BOOL)
    c.getCompressionAlgorithm should === (CompressionAlgorithm.LZ4)
  }

  it should "put keys first as defined in the config" in {
    val converter = new Converter {
      override def kuduColumns(): List[KuduColumnBuilder] = {
        List(
          KuduColumnBuilder("rest", new ColumnSchemaBuilder("rest", Type.STRING)),
          KuduColumnBuilder("name", new ColumnSchemaBuilder("name", Type.STRING)),
          KuduColumnBuilder("active", new ColumnSchemaBuilder("active", Type.BOOL)),
          KuduColumnBuilder("id", new ColumnSchemaBuilder("id", Type.INT64))
        )
      }
    }
    {
      val List(a, b, c, d) = CreateTable.buildKuduColumns(converter, List("id", "name"), false)
      a.getName should ===("id")
      b.getName should ===("name")
      c.getName should ===("rest")
      d.getName should ===("active")
    }
    {
      val List(a, b, c, d) = CreateTable.buildKuduColumns(converter, List("name", "id"), false)
      a.getName should ===("name")
      b.getName should ===("id")
      c.getName should ===("rest")
      d.getName should ===("active")
    }
    {
      val List(a, b, c, d) = CreateTable.buildKuduColumns(converter, List("active", "id", "name"), false)
      a.getName should ===("active")
      b.getName should ===("id")
      c.getName should ===("name")
      d.getName should ===("rest")
    }
  }

  it should "build multiple Kudu column keys properly" in {
    val converter = new Converter {
      override def kuduColumns(): List[KuduColumnBuilder] = {
        List(
          KuduColumnBuilder("name", new ColumnSchemaBuilder("name", Type.STRING)),
          KuduColumnBuilder("active", new ColumnSchemaBuilder("active", Type.BOOL)),
          KuduColumnBuilder("id", new ColumnSchemaBuilder("id", Type.INT64))
        )
      }
    }

    val keys = List[HashedKey]("id", "name")
    val List(a, b, c) = CreateTable.buildKuduColumns(converter, keys, compressed = true)

    a.getName should === ("id")
    a.isKey should === (true)

    b.getName should === ("name")
    b.isKey should === (true)

    c.getName should === ("active")
    c.isKey should === (false)
  }

}
