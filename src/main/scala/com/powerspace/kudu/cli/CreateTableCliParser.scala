package com.powerspace.kudu.cli

import com.powerspace.kudu.{CreateTableConfig, HashedKey}

object CreateTableCliParser {
  import CLiReads._

  def parse(args: Seq[String]): Option[CreateTableConfig] = parser.parse(args, CreateTableConfig())

  implicit def parser = new scopt.OptionParser[CreateTableConfig]("kudu-from-avro") {
    opt[String]('t', "table").required()
      .action((x, c) => c.copy(tableName = x))
      .text("Table to create in Kudu")

    opt[List[HashedKey]]('p', "primary_key").required()
      .action((x, c) => c.copy(pkeys = x))
      .text("Primary key column name in the Kudu table")

    opt[Int]('r', "replica")
      .action((x, c) => c.copy(replica = x))
      .text("Number of replicas (default: 3)")

    opt[Int]('b', "buckets")
      .action((x, c) => c.copy(replica = x))
      .text("Number of buckets (default: 32)")

    opt[String]('s', "avro_schema")
      .action((x, c) => c.copy(avroSchemaPath = Some(x)))
      .text(".avsc to read to create the table")

    opt[Boolean]('c', "compressed")
      .action((x, c) => c.copy(compressed = x))
      .text("Compress columns using LZ4")

    opt[List[String]]('k', "kudu_servers").required()
      .action((x, c) => c.copy(kuduServers = x))
      .text("Kudu master tablets")

    opt[String]('q', "sql")
      .action((x, c) => c.copy(sql = Some(x)))
      .text("Custom SQL creation to create columns from: \"id INTEGER, ts BIGINT, name STRING\"")

    checkConfig(c =>
      if (c.sql.isDefined && c.avroSchemaPath.isDefined) {
        failure("Can't use an SQL and an Avro schema at the same time")
      } else {
        success
      }
    )
  }
}