package com.powerspace.kudu.cli

import com.powerspace.kudu.DropColumnConfig

/**
  * Created by valdo404 on 26/05/2017.
  */
object DropColumnCliParser {
  import CLiReads._

  def parse(args: Seq[String]): Option[DropColumnConfig] = parser.parse(args, DropColumnConfig())

  implicit def parser = new scopt.OptionParser[DropColumnConfig]("kudu-from-avro") {
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

    checkConfig(c => success)
  }
}
