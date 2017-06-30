# Kudu from Avro and SQL [![Build Status](https://travis-ci.org/Powerspace/kudu-from-avro.svg?branch=master)](https://travis-ci.org/Powerspace/kudu-from-avro)

This tool can create a Kudu table from an Avro schema or from a (Impala) SQL script. (the SQL part came later :-))

# Usage

```
Usage: 

create-table [options]

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
  
update-table [options]

  -t, --table <value>      Table to update in Kudu
  -k, --kudu_servers <value>
                           Kudu master tablets
  -n, --name <value>       Name of the column to update
  --type <value>           Type of the column (one of int8, int16, int32, int64, binary,
                                                string, bool, float, double, unixtime_micros)
  -c, --compression        Set the compression to LZ4
  --nullable               Set the column to nullable
  --raw_key                Set the column as part of the row key
```

## Compound keys

`-p` supports a compound primary key `-p id,company_id`; those columns will be the first in the Kudu table, as required by Kudu. It also support a value to specify the count of partitions: `-pd id:100,company_id:10`

# Create a Kudu table from an Avro schema
 
```
$ ./create-table -t my_new_table -p id -s schema.avsc -k kudumaster01
```

# Create a Kudu table from a SQL script

Note that it defaults all columns to _nullable_ (except the keys of course).

```
$ ./create-table -q "id STRING, ts BIGINT, name STRING" -t my_new_table -p id -k kudumaster01
```

# Add a column to a Kudu Table

```
$ ./update-table -t my_table -n new_column --type float -c --nullable --raw_key -k kudumaster01
```

# How to build it

```
$ sbt universal:packageBin
```

The `.zip` will be available in `target/universal/kudu-from-avro-1.0.zip`, and the executable inside: `bin`, `update-table` and `create-table`.

