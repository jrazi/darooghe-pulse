# DarooghePulse

DarooghePulse is a containerized transaction event generator designed for the Data Science course assignments at the University of Tehran, Spring of 2025 semester. It provides a ready-to-use Kafka environment that streams synthetic payment transaction events, facilitating downstream projects involving real-time data processing and analytics with Apache Spark and related tools.

## Components

- **Kafka Broker**: Distributes transaction event streams to downstream consumers.
- **Kafdrop**: Offers web-based monitoring of Kafka topics, partitions, and messages.
- **Event Producer**: Generates realistic transaction events, initializing Kafka with historical data (approx. 20,000 events from the past 7 days), followed by continuous real-time event production.

## Key Features

- **Platform Compatibility**: Supports Ubuntu AMD64 and MacBook Apple Silicon.
- **Historical Data Initialization**: Provides a realistic dataset for immediate analytical tasks.
- **Configurable Event Rate**: Easily adjustable parameters for varying event throughput and fraud rate simulations.
