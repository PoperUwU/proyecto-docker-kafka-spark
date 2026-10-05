from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Lee los mensajes del topic en tiempo real
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "actividad-topic")
      .option("startingOffsets", "earliest")
      .load())

# Los mensajes de Kafka vienen en binario, hay que convertirlos a texto
mensajes = df.selectExpr("CAST(value AS STRING) as mensaje")

# Muestra los mensajes en consola a medida que llegan
query = (mensajes.writeStream
         .format("console")
         .outputMode("append")
         .start())

query.awaitTermination()
