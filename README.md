# Kudu from Avro

This tool can create a Kudu table from an Avro schema.

## Usage

```
Usage: CreateTable [options]

  -t, --table <value>      Table to create in Kudu
  -p, --primary_key <value>
                           Primary key column name in the Kudu table
  -r, --replica <value>    Number of replicas (default: 3)
  -b, --buckets <value>    Number of buckets (default: 32
  -s, --avro_schema <value>
                           .avsc to read to create the table
  -c, --compressed <value>
                           Compress columns using LZ4
  -k, --kudu_servers <value>
                           Kudu master tablets
```

## Example

```
sbt "run -t my_new_table -p id -s schema.avsc -k kudumaster01"
```