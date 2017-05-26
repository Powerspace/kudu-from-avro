package com.powerspace.kudu.converters

import org.apache.kudu.ColumnSchema
import org.apache.kudu.ColumnSchema.{ColumnSchemaBuilder, CompressionAlgorithm}

case class SqlColumn(name: String, kind: String)
case class KuduColumnBuilder(name: String, builder: ColumnSchemaBuilder)

trait ColumnBuilder {
  protected def baseColumns(): List[(String, ColumnSchemaBuilder)]

  def kuduColumns(
                   compressionAlgorithm: CompressionAlgorithm,
                   keys: List[String]): List[ColumnSchema] = {
    baseColumns().map{ case(name, builder) => builder.
        compressionAlgorithm(compressionAlgorithm).
        key(keys.contains(name)).build()
      }.sortWith(sortColumns(_,_)(keys))
  }

  /** Compare schemas using key properties */
  def sortColumns(left: ColumnSchema, right: ColumnSchema)(pkeys: List[String]): Boolean = {
    if (!left.isKey) false
    else if (!right.isKey) true
    else pkeys.indexOf(left.getName) < pkeys.indexOf(right.getName)
  }
}
