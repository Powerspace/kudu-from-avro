package com.powerspace.kudu

import com.powerspace.kudu.cli.AlterTableCliParser
import org.apache.kudu.client.AlterTableOptions
import org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder
import org.slf4j.LoggerFactory

case class DropColumnConfig(
                             tableName: String = "demo",
                             kuduServers: List[String] = List(),
                             columnName: String = "test") {
}

/**
  * Drop a specific column
  */
object DropColumn extends App {
  import scala.collection.JavaConverters._

  val logger = LoggerFactory.getLogger(getClass)

  AlterTableCliParser.parse(args) match {
    case Some(config) => updateTable(config)
    case None =>
  }

  def updateTable(config: AddColumnConfig): Unit = {
    logger.info(config.toString)

    val options = buildAlterTableOptions(config)

    val newTableName = config.tableName

    logger.info(s"Altering table $newTableName...")
    val client = new AsyncKuduClientBuilder(config.kuduServers.asJava).build()
    println(client.alterTable(config.tableName, options).join())
    logger.info(s"Table $newTableName altered !")
  }

  def buildAlterTableOptions(config: AddColumnConfig): AlterTableOptions = {
    new AlterTableOptions().dropColumn(config.columnName)
  }
}
