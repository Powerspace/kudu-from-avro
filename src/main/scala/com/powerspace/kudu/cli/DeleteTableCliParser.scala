package com.powerspace.kudu.cli

import com.powerspace.kudu.DeleteTableConfig

object DeleteTableCliParser {

  import CLiReads._

  def parse(args: Seq[String]): Option[DeleteTableConfig] = parser.parse(args, DeleteTableConfig())

  implicit def parser = new scopt.OptionParser[DeleteTableConfig]("kudu-from-avro") {
    opt[String]('t', "table").required()
      .action((x, c) => c.copy(tableName = x))
      .text("Table to create in Kudu")

    opt[List[String]]('k', "kudu_servers").required()
      .action((x, c) => c.copy(kuduServers = x))
      .text("Kudu master tablets")

    checkConfig(_ => success)
  }
}
