package com.powerspace.kudu.converters

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.kudu.{ColumnSchema, Type}
import org.apache.kudu.ColumnSchema.{ColumnSchemaBuilder, CompressionAlgorithm}

import collection.JavaConverters._

case class AvroColumn(name: String, schema: Schema)

object AvroColumnBuilder {
  def apply(avroPath: String): AvroColumnBuilder = new AvroColumnBuilder(avroPath)

  def fieldToAvroColumn(field: Field) = AvroColumn(field.name(), field.schema())
}

class AvroColumnBuilder(avroSchema: String) extends ColumnBuilder {

  val schema: Schema = new Schema.Parser().parse(avroSchema)

  override def baseColumns() = {
    avroColumns().map(
      field => (field.name, toKuduColumn(field.name, field.schema)))
  }


  def avroColumns(): List[AvroColumn] = {
    schema.getFields.asScala.map(AvroColumnBuilder.fieldToAvroColumn).toList
  }

  private def toKuduColumn(name: String, avroSchema: org.apache.avro.Schema): ColumnSchemaBuilder = {
    import org.apache.avro.Schema.Type._

    avroSchema.getType match {
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
            throw new IllegalArgumentException(s"Unsupported union type ${avroSchema.getType}")
          }
        } else {
          throw new IllegalArgumentException(s"Unsupported union type ${avroSchema.getType}")
        }

      case other => throw new IllegalArgumentException(s"Unsupported type $other")
    }

  }

}