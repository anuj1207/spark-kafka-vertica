# spark-kafka-vertica
A template for connecting Vertica and Kafka using Spark (Batch mode and Structured Streaming)

This project has two applications

1. StructuredStreamingKafkaToVerticaApplication
2. VerticaToKafkaApplication
***
### StructuredStreamingKafkaToVerticaApplication
This application provides a template for a **Structured Streaming**
application which **reads data from a Kafka topic** and **insert that
data into Vertica** Database.
#### watermarking and deduplication
This application has already written functions for watermarking and
deduplication **ready to be plugged in** and custom models can be used
with this application. Also user can write some transformation and
aggregation logic in the space provided to be used with custom models.

**Note**: Vertica doesn't support structured streaming by default hence a
customWriter has been used while writing to Vertica called
"foreachBatch" which is introduced in Spark 2.4. For other data sources
which have compatibility for Structured Streaming they can be used
directly without any custom writing

### VerticaToKafkaApplication
Another application which is provided by this project is a **batch mode
application** and it **reads from Vertica** Database and **puts data
into Kafka** sink topic.

This application simply uses the Spark SQL i.e **reads a DataFrame from
Vertica** and writes that DataFrame into Kafka topic. Between this read
and write a custom logic for transformation can be written based on your
custom models
***

This project also includes **two DataSources Vertica and Kafka
DataSource** separately and these data sources can be combined with each
other for ex: (Vertica => Vertica) or (Kafka => Kafka). Also, there's a
**`Source` trait** defined in the project **which can be used to implement
other data sources**.

**Prerequisites**:

1. Run Kafka: provide host and port in config file or environment
2. Run HDFS: provide name node and webdfs node in config file or environment
3. Run Vertica: provide host, port and credentials for vertica in config file or environment
4. Run corresponding application