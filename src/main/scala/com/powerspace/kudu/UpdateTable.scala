package com.powerspace.kudu

import com.powerspace.kudu.cli.AlterTableCliParser
import org.apache.kudu.ColumnSchema.CompressionAlgorithm
import org.apache.kudu.client.AlterTableOptions
import org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder
import org.apache.kudu.{ColumnSchema, Type}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


case class AddColumnConfig(
                   tableName: String = "demo",
                   kuduServers: List[String] = List(),
                   columnName: String = "test",
                   nullable: Boolean = false,
                   columnType: Type = Type.STRING,
                   key: Boolean = false,
                   compression: CompressionAlgorithm = CompressionAlgorithm.LZ4,
                   default: Option[Any] = None) {

}

object UpdateTable extends App {
  val logger = LoggerFactory.getLogger(getClass)

  AlterTableCliParser.parse(args) match {
    case Some(config) => updateTable(config)
    case None =>
  }

  def updateTable(config: AddColumnConfig): Unit = {
    logger.info(config.toString)

    val options = buildAlterTableOptions(config)

    val tableName = config.tableName

    logger.info(s"Updating table $tableName...")
    val client = new AsyncKuduClientBuilder(config.kuduServers.asJava).build()
    client.alterTable(config.tableName, options)
    logger.info(s"Table $tableName altered !")
  }

  def buildAlterTableOptions(config: AddColumnConfig): AlterTableOptions = {
    new AlterTableOptions().addColumn(addColumn(config))
  }

  private def addColumn(config: AddColumnConfig) = {
    new ColumnSchema.ColumnSchemaBuilder(config.columnName, config.columnType).
      compressionAlgorithm(config.compression).
      key(config.key).
      nullable(config.nullable).
      build()
  }
}
