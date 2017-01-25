package com.powerspace.kudu

import com.powerspace.kudu.converters.{Converter, KuduColumnBuilder}
import org.apache.kudu.ColumnSchema.{ColumnSchemaBuilder, CompressionAlgorithm}
import org.apache.kudu.Type
import org.scalatest.{FlatSpec, Matchers}

class CreateTableTest extends FlatSpec with Matchers {

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

    val List(a, b, c) = CreateTable.buildKuduColumns(converter, "id", compressed = true)

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

}
