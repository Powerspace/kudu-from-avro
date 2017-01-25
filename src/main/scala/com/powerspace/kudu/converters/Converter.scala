package com.powerspace.kudu.converters

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder

case class SqlColumn(name: String, kind: String)
case class KuduColumnBuilder(name: String, builder: ColumnSchemaBuilder)

trait Converter {
  def kuduColumns(): List[KuduColumnBuilder]
}
