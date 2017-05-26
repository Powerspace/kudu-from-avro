package com.powerspace.kudu.converters

import org.apache.kudu.ColumnSchema.{ColumnSchemaBuilder, CompressionAlgorithm}
import org.apache.kudu.{ColumnSchema, Type}

object SqlColumnBuilder {
  def apply(sql: String, pkeys: List[String]): SqlColumnBuilder = new SqlColumnBuilder(sql, pkeys)
}

class SqlColumnBuilder(sql: String, pkeys: List[String]) extends ColumnBuilder {
  override def baseColumns(): List[(String, ColumnSchemaBuilder)] = {
    sqlColumns().map(c => (c.name, toKuduColumn(c.name, c.kind)))
  }

  def sqlColumns(): List[SqlColumn] = {
    val splittedVar = sql.split(',').
      flatMap(_.split(' ')).
      filter(_.nonEmpty)

    val nameAndKinds = splittedVar.
      grouped(2).map(_.toList)

    nameAndKinds.
      map { case List(name: String, kind: String) => SqlColumn(name, kind)
    }.toList
  }

  private def toKuduColumn(name: String, sqlType: String): ColumnSchemaBuilder = {
    (sqlType match {
      case "INT" => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT32)
      case "BIGINT" => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT64)
      case "STRING" => new ColumnSchema.ColumnSchemaBuilder(name, Type.STRING)
      case "BOOLEAN" => new ColumnSchema.ColumnSchemaBuilder(name, Type.BOOL)
      case "DOUBLE" => new ColumnSchema.ColumnSchemaBuilder(name, Type.DOUBLE)
      case "FLOAT" => new ColumnSchema.ColumnSchemaBuilder(name, Type.FLOAT)
      //case "BYTES" => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY)
      //case "FIXED" => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY)
      case other => throw new IllegalArgumentException(s"Unsupported type $other")
    }).key(pkeys.contains(name)).nullable(!pkeys.contains(name))
  }

}