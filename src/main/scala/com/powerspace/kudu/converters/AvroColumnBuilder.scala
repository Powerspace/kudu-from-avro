package com.powerspace.kudu.converters

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{ColumnSchema, Type}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable

case class AvroColumn(name: String, schema: Schema)

object AvroColumnBuilder {
  def apply(rawSchema: String): AvroColumnBuilder = new AvroColumnBuilder(new Schema.Parser().parse(rawSchema))

  def fieldToAvroColumn(field: Field) = AvroColumn(field.name(), field.schema())
}

object SchemaAnalysis {
  def toKuduColumn(field: Field): (String, ColumnSchemaBuilder) = (field.name(), recurseGeneration(field.name(), field.schema(), field.defaultVal()))

  private def recurseGeneration(name: String, avroSchema: org.apache.avro.Schema, defaultValue: AnyRef): ColumnSchema.ColumnSchemaBuilder = {
    import org.apache.avro.Schema.Type._


    avroSchema.getType match {
      case INT => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT32).defaultValue(defaultValue)
      case STRING => new ColumnSchema.ColumnSchemaBuilder(name, Type.STRING).defaultValue(defaultValue)
      case BOOLEAN => new ColumnSchema.ColumnSchemaBuilder(name, Type.BOOL).defaultValue(defaultValue)
      case BYTES => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY).defaultValue(defaultValue)
      case DOUBLE => new ColumnSchema.ColumnSchemaBuilder(name, Type.DOUBLE).defaultValue(defaultValue)
      case FLOAT => new ColumnSchema.ColumnSchemaBuilder(name, Type.FLOAT).defaultValue(defaultValue)
      case LONG => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT64).defaultValue(defaultValue)
      case FIXED => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY).defaultValue(defaultValue)
      case ENUM => new ColumnSchema.ColumnSchemaBuilder(name, Type.STRING).defaultValue(defaultValue)
      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            recurseGeneration(name, remainingUnionTypes.head, defaultValue).nullable(true)
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


class AvroColumnBuilder(schema: Schema) extends ColumnBuilder {
  import SchemaAnalysis._

  override def baseColumns() = {
    fields().map(field => toKuduColumn(field))
  }

  def fields(): List[Field] = {
    schema.getFields.asScala.toList
  }
}