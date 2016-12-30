package com.powerspace.kudu

import org.apache.avro.Schema
import org.apache.kudu.ColumnSchema.CompressionAlgorithm
import org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.{ColumnSchema, Type}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source

case class Config(
                 tableName: String = "demo",
                 pkey: String = "id",
                 avroSchemaPath: String = "",
                 kuduServers: List[String] = List(),
                 compressed: Boolean = true,
                 replica: Int = 3,
                 buckets: Int = 32
                 )

object CreateTable extends App {
  val logger = LoggerFactory.getLogger(getClass)

  implicit val listRead: scopt.Read[List[String]] =
    scopt.Read.reads(x => x.split(',').toList)

  val parser = new scopt.OptionParser[Config]("CreateTable") {
    opt[String]('t', "table").required()
      .action((x, c) => c.copy(tableName = x))
      .text("Table to create in Kudu")

    opt[String]('p', "primary_key").required()
      .action((x, c) => c.copy(pkey = x))
      .text("Primary key column name in the Kudu table")

    opt[Int]('r', "replica").required()
      .action((x, c) => c.copy(replica = x))
      .text("Number of replicas (default: 3)")

    opt[Int]('b', "buckets").required()
      .action((x, c) => c.copy(replica = x))
      .text("Number of buckets (default: 32")

    opt[String]('s', "avro_schema").required()
      .action((x, c) => c.copy(avroSchemaPath = x))
      .text(".avsc to read to create the table")

    opt[Boolean]('c', "compressed").required()
      .action((x, c) => c.copy(compressed = x))
      .text("Compress columns using LZ4")

    opt[List[String]]('k', "kudu_servers").required()
      .action((x, c) => c.copy(kuduServers = x))
      .text("Kudu master tablets")
  }
  parser.parse(args, Config()) match {
    case Some(config) => createTable(config)
    case None =>
  }

  def createTable(config: Config) = {

    logger.info(config.toString)

    val newTableName = config.tableName
    val pkey = config.pkey
    val schema = new Schema.Parser().parse(Source.fromFile(config.avroSchemaPath).mkString)

    def toKuduColumn(name: String, avroSchema: org.apache.avro.Schema): ColumnSchema.ColumnSchemaBuilder = {
      import org.apache.avro.Schema.Type._

      val t = avroSchema.getType
      t match {
        case INT => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT32)
        case STRING => new ColumnSchema.ColumnSchemaBuilder(name, Type.STRING)
        case BOOLEAN => new ColumnSchema.ColumnSchemaBuilder(name, Type.BOOL)
        case BYTES => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY)
        case DOUBLE => new ColumnSchema.ColumnSchemaBuilder(name, Type.DOUBLE)
        case FLOAT => new ColumnSchema.ColumnSchemaBuilder(name, Type.FLOAT)
        case LONG => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT64)
        case FIXED => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY)
        case ENUM => new ColumnSchema.ColumnSchemaBuilder(name, Type.STRING)
        case UNION =>
          if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
            // In case of a union with null, eliminate it and make a recursive call
            val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
            if (remainingUnionTypes.size == 1) {
              toKuduColumn(name, remainingUnionTypes.head).nullable(true)
            } else {
              throw new IllegalArgumentException(s"Unsupported union type $t")
            }
          } else {
            throw new IllegalArgumentException(s"Unsupported union type $t")
          }

        case other => throw new IllegalArgumentException(s"Unsupported type $other")
      }

    }

    def buildKuduStructure(key: String): List[ColumnSchema] = {
      implicit def orderingByName[A <: ColumnSchema]: Ordering[A] =
        Ordering.by(!_.isKey)

      schema.getFields.asScala.map(field => toKuduColumn(field.name, field.schema())
        .compressionAlgorithm(if(config.compressed) CompressionAlgorithm.LZ4 else CompressionAlgorithm.NO_COMPRESSION)
        .key(field.name() == pkey)
        .build()).toList.sorted
    }

    val options = new CreateTableOptions().addHashPartitions(List(pkey).asJava, config.buckets).setNumReplicas(config.replica)

    logger.info(s"Creating table $newTableName...")
    val client = new AsyncKuduClientBuilder(config.kuduServers.asJava).build()
    client.createTable(newTableName, new org.apache.kudu.Schema(buildKuduStructure(pkey).asJava), options).join()
    logger.info(s"Table $newTableName created!")
  }

}
