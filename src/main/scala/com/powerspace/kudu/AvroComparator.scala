package com.powerspace.kudu

import com.powerspace.kudu.converters.SchemaAnalysis.toKuduColumn
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.AlterTableOptions

object AvroComparator {
  import scala.collection.JavaConverters._

  /** Compare two schemas and build alter table specification **/
  def compareAndAddFields(previousSchema: Schema, newSchema: Schema): AlterTableOptions = {
    val initial = new AlterTableOptions()

    val withAddings = columnsToAdd(previousSchema, newSchema).
      foldLeft(initial)((acc: AlterTableOptions, key: (String, ColumnSchemaBuilder)) =>
      acc.addColumn(
        key._2.
        build())
    )

    withAddings
  }

  /** Compare two schemas and build alter table specification **/
  def compareAndRemoveFields(previousSchema: Schema, newSchema: Schema): AlterTableOptions = {
    val initial = new AlterTableOptions()

    val withRemoves = toRemove(previousSchema, newSchema).foldLeft(initial)((acc: AlterTableOptions, field: Field) =>
      acc.dropColumn(field.name()))

    withRemoves
  }

  /** columns to add **/
  def columnsToAdd(previousSchema: Schema, newSchema: Schema): Seq[(String, ColumnSchemaBuilder)] = {
    toAdd(previousSchema, newSchema).map(toKuduColumn)
  }

  /** fields to add **/
  def toAdd(previousSchema: Schema, newSchema: Schema) = {
    newSchema.getFields.asScala.
      map(field => (field, Option(previousSchema.getField(field.name())))).
      collect { case (field, None) => field }
  }

  /** fields to remove **/
  def toRemove(previousSchema: Schema, newSchema: Schema) = {
    previousSchema.getFields.asScala.
      map(field => (field, Option(newSchema.getField(field.name())))).
      collect { case (field, None) => field }
  }
}
