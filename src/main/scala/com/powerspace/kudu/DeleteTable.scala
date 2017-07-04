package com.powerspace.kudu

import com.powerspace.kudu.cli.DeleteTableCliParser
import org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


case class DeleteTableConfig(
                              tableName: String = "demo",
                              kuduServers: List[String] = List()
                            )

object DeleteTable extends App {
  val logger = LoggerFactory.getLogger(getClass)

  DeleteTableCliParser.parse(args) match {
    case Some(config) => deleteTable(config)
    case None =>
  }

  def deleteTable(config: DeleteTableConfig): Unit = {
    logger.info(config.toString)

    val table = config.tableName
    logger.info(s"Deleting table $table ...")
    val client = new AsyncKuduClientBuilder(config.kuduServers.asJava).build()
    client.deleteTable(table).join()
    logger.info(s"Table $table deleted!")
  }
}
