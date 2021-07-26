# Streaming Flight Data

This directory has code and schemas for interacting with the streaming data from OpenSky.

The python folder contains code to use with Cloud Function that will pull the latest data from OpenSky and store each
record as a separate file in GCS. The schema folder has definitions for data content for use by BigQuery and/or 
DataFlow.

## Schemas

The schema folder has schemas used in BigQuery and in connecting a Pub/Sub topic to a BigQuery table.
BigQuery schemas are used when you create a table from scratch. DataFlow jobs that transfer messages from a Pub/Sub 
topic to BigQuery, creating a table if it does not exist or appending to an existing table, will need a schema that 
defines the contents of the messages in the topic. The schema needed by DataFlow uses different types than BigQuery.

**bqDataFlow_opensky_noAvroStringDates_schmea.json** -- this is a schema that DataFlow can recognize which defines the
fields in the OpenSky records in a Pub/Sub topic. The types used in this schema will not be recognized by BigQuery,
only by DataFlow.

**not_used** -- a directory of other schemas used for various tools which are not necessarily needed. The Avro schemas
are used when applying a schema to a Pub/Sub topic. Avro schema is yet another type of schema that is not compatible
with BigQuery nor with DataFlow.

**not_used/bqDestination_opensky_noAvro_schema.json** -- you may find this schema helpful if you want to create a
table linked to a folder in a GCS bucket which is filled with records from OpenSky.