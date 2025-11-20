# Kafka Avro Retry & DLQ System

A Kafka-based system demonstrating order message processing with **Avro serialization**, **real-time aggregation**, **retry logic**, and **Dead Letter Queue (DLQ)** handling.

## Features

- **Avro Serialization**: Messages serialized using Apache Avro schema
- **Real-time Aggregation**: Running average calculation of product prices
- **Retry Logic**: 3 automatic retry attempts for temporary failures
- **Dead Letter Queue**: Failed messages routed to DLQ after max retries
- **Docker Compose**: Complete Kafka ecosystem (Kafka, Zookeeper, Schema Registry)
