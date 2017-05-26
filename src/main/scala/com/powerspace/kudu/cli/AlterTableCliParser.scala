package com.powerspace.kudu.cli

import com.powerspace.kudu.{AddColumnConfig, CreateTableConfig, HashedKey}
import org.apache.kudu.ColumnSchema.CompressionAlgorithm
import org.apache.kudu.Type

object AlterTableCliParser {
  import CLiReads._

  def parse(args: Seq[String]): Option[AddColumnConfig] = parser.parse(args, AddColumnConfig())

  implicit def parser = new scopt.OptionParser[AddColumnConfig]("kudu-from-avro") {
    opt[String]('t', "table").required()
      .action((x, c) => c.copy(tableName = x))
      .text("Table to update in Kudu")

    opt[List[String]]('k', "kudu_servers").required()
      .action((x, c) => c.copy(kuduServers = x))
      .text("Kudu master tablets")

    opt[String]("name").required()
      .action((x, c) => c.copy(
        columnName = x))
      .text("Column name")

    opt[String]("type").required()
      .action((x, c) => c.copy(
        columnType = Type.valueOf(x)))
      .text("Column type")

    opt[Boolean]("compression")
      .action((x, c) => c.copy(
        compression = if(x) CompressionAlgorithm.LZ4 else CompressionAlgorithm.NO_COMPRESSION))
      .text("Compress columns using LZ4")

    opt[Boolean]("nullable")
      .action((x, c) => c.copy(
        nullable = x))
      .text("Nullable")

    opt[Boolean]("raw_key")
      .action((x, c) => c.copy(
        key = x))
      .text("Raw key")

    checkConfig(c => success)
  }
}