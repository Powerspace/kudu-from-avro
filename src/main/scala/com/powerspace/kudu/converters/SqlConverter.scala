package com.powerspace.kudu.converters

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{ColumnSchema, Type}

object SqlConverter {
  def apply(sql: String, pkey: String): SqlConverter = new SqlConverter(sql, pkey)
}

class SqlConverter(sql: String, pkey: String) extends Converter {

  override def kuduColumns(): List[KuduColumnBuilder] = {
    sqlColumns().map(c => KuduColumnBuilder(c.name, toKuduColumn(c.name, c.kind)))
  }

  def sqlColumns(): List[SqlColumn] = {
    sql.split(',').flatMap(_.split(' ')).filter(_.nonEmpty).grouped(2).map(_.toList).map { case List(name: String, t: String) =>
      SqlColumn(name, t)
    }.toList
  }

  private def toKuduColumn(name: String, t: String): ColumnSchemaBuilder = {
    (t match {
      case "INT" => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT32)
      case "BIGINT" => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT64)
      case "STRING" => new ColumnSchema.ColumnSchemaBuilder(name, Type.STRING)
      case "BOOLEAN" => new ColumnSchema.ColumnSchemaBuilder(name, Type.BOOL)
      case "DOUBLE" => new ColumnSchema.ColumnSchemaBuilder(name, Type.DOUBLE)
      case "FLOAT" => new ColumnSchema.ColumnSchemaBuilder(name, Type.FLOAT)
      //case "BYTES" => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY)
      //case "FIXED" => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY)
      case other => throw new IllegalArgumentException(s"Unsupported type $other")
    }).nullable(name != pkey)
  }

}