# Kudu from Avro and SQL

This tool can create a Kudu table from an Avro schema or from a (Impala) SQL script. (the SQL part came later :-))

# Usage

```
Usage: CreateTable [options]

  -t, --table <value>      Table to create in Kudu
  -p, --primary_key <value>
                           Primary key column name in the Kudu table
  -r, --replica <value>    Number of replicas (default: 3)
  -b, --buckets <value>    Number of buckets (default: 32)
  -s, --avro_schema <value>
                           .avsc to read to create the table
  -c, --compressed <value>
                           Compress columns using LZ4
  -k, --kudu_servers <value>
                           Kudu master tablets
  -q, --sql <value>        Custom SQL creation to create columns from: "id INTEGER, ts BIGINT, name STRING"
```

# Create a Kudu table from an Avro schema
 
```
sbt "run -t my_new_table -p id -s schema.avsc -k kudumaster01"
```

# Create a Kudu table from a SQL script

Note that it defaults all columns to _nullable_ (except the keys of course).

```
sbt "run -q \"id STRING, ts BIGINT, name STRING\" -t my_new_table -p id -k kudumaster01"
```

