package com.powerspace.kudu

import com.powerspace.kudu.cli.AlterTableCliParser
import org.apache.kudu.ColumnSchema.CompressionAlgorithm
import org.apache.kudu.client.AlterTableOptions
import org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder
import org.apache.kudu.{ColumnSchema, Type}
import org.slf4j.LoggerFactory



case class AddColumnConfig(
                   tableName: String = "demo",
                   kuduServers: List[String] = List(),
                   columnName: String = "test",
                   nullable: Boolean = false,
                   columnType: Type = Type.STRING,
                   key: Boolean = false,
                   compression: CompressionAlgorithm = CompressionAlgorithm.LZ4,
                   default: Option[Either[Number, String]] = None) {
}



/**
  * Add a specific column
  */
object AddColumn extends App {
  import scala.collection.JavaConverters._

  val logger = LoggerFactory.getLogger(getClass)

  AlterTableCliParser.parse(args) match {
    case Some(config) => updateTable(config)
    case None =>
  }

  def updateTable(config: AddColumnConfig): Unit = {
    logger.info(config.toString)

    val options = new AlterTableOptions().addColumn(addColumn(config))

    val newTableName = config.tableName

    logger.info(s"Altering table $newTableName...")
    val client = new AsyncKuduClientBuilder(config.kuduServers.asJava).build()
    println(client.alterTable(config.tableName, options).join())
    logger.info(s"Table $newTableName altered !")
  }

  /** Extract the good value with the good type given the wanted kudu type **/
  def defaultToType(kuduType: Type, default: Either[Number, String]): Any = {
    val numerical = default.left.toOption
    val unparsed = default.right.toOption

    kuduType match {
      case Type.INT32 => numerical.map(_.intValue()).orNull
      case Type.FLOAT => numerical.map(_.floatValue()).orNull
      case Type.DOUBLE => numerical.map(_.doubleValue()).orNull
      case Type.STRING =>
        numerical.map(_.toString).orElse(unparsed).orNull
    }
  }

  private def addColumn(config: AddColumnConfig) = {
    val baseBuilder = new ColumnSchema.ColumnSchemaBuilder(config.columnName, config.columnType).
      compressionAlgorithm(config.compression).
      key(config.key).
      nullable(config.nullable)

    config.default.map(d => baseBuilder.defaultValue(defaultToType(config.columnType, d))).
      getOrElse(baseBuilder).
      build()
  }
}


