package com.powerspace.kudu.converters

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{ColumnSchema, Type}

object SqlConverter {
  def apply(sql: String): SqlConverter = new SqlConverter(sql)
}

class SqlConverter(sql: String) extends Converter {

  override def kuduColumns(): List[KuduColumnBuilder] = {
    sqlColumns().map(c => KuduColumnBuilder(c.name, toKuduColumn(c.name, c.kind)))
  }

  // "transaction_type STRING,    ts BIGINT,    transaction_uuid STRING,    user_agent STRING,    ip STRING,    resolved_ip STRING,    carrier STRING,    connection_type STRING,    line_speed STRING,    ip_routing_type STRING,    isp_domain_name STRING,    url_scheme STRING,    language STRING,    psuid STRING,    pstuid STRING,    md5 STRING,    domain_email STRING,    city_name STRING,    city_geonames_id INT,    countrycode STRING,    latitude FLOAT,    longitude FLOAT,    timezone STRING,    admin1 STRING,    admin2 STRING,    state_code STRING,    is_valid_transaction BOOLEAN,    alias_of STRING,    event_hash STRING,    query_string STRING,    position_id INT,    position_short_code STRING,    publisher_id INT,    website_id INT,    adtemplate_id INT,    advertiser_id INT,    adgroup_id INT,    adcopy_id INT,    zone INT,    campaign_id INT,    algo STRING,    redirect_url STRING,    cpc FLOAT,    payout FLOAT,    revenue_share FLOAT,    pws_roi FLOAT,    monthly_budget INT,    order_value FLOAT,    order_currency STRING,    order_id STRING,    day INT,    month INT,    year INT,    hour INT,    day_of_week STRING,    week_of_year INT,    device_type STRING,    device_name STRING,    device_version STRING,    browser_name STRING,    browser_version STRING,    browser_minor_version STRING,    os STRING,    os_version STRING,    os_minor_version STRING,    screen_resolution STRING,    incoming_impression_uuid STRING,    position_impression STRING,    predicted_ctr FLOAT,    predicted_cpm FLOAT"
  def sqlColumns(): List[SqlColumn] = {
    sql.split(',').flatMap(_.split(' ')).filter(_.nonEmpty).grouped(2).map(_.toList).map { case List(name: String, t: String) =>
      SqlColumn(name, t)
    }.toList
  }

  private def toKuduColumn(name: String, t: String): ColumnSchemaBuilder = {
    t match {
      case "INT" => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT32)
      case "BIGINT" => new ColumnSchema.ColumnSchemaBuilder(name, Type.INT64)
      case "STRING" => new ColumnSchema.ColumnSchemaBuilder(name, Type.STRING)
      case "BOOLEAN" => new ColumnSchema.ColumnSchemaBuilder(name, Type.BOOL)
      case "DOUBLE" => new ColumnSchema.ColumnSchemaBuilder(name, Type.DOUBLE)
      case "FLOAT" => new ColumnSchema.ColumnSchemaBuilder(name, Type.FLOAT)
      //case "BYTES" => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY)
      //case "FIXED" => new ColumnSchema.ColumnSchemaBuilder(name, Type.BINARY)
      case other => throw new IllegalArgumentException(s"Unsupported type $other")
    }
  }

}