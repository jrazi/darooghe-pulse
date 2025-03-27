# DarooghePulse

DarooghePulse is a containerized transaction event generator designed for the Data Science course assignments at the University of Tehran, Spring 2025 semester. It provides a ready-to-use Kafka environment that streams synthetic payment transaction events, facilitating real-time data processing and analytics with Apache Spark and related tools.

## Components

- **Kafka Broker**: Distributes transaction event streams to downstream consumers.
- **Kafdrop**: Web-based interface for monitoring Kafka topics, partitions, and messages.
- **Event Producer (DarooghePulse)**: Generates realistic transaction events by initializing Kafka with historical data (~20,000 events over the past 7 days) and then continuously producing real-time events.

## Key Features

- **Historical Data Initialization**: Provides a realistic dataset ready for immediate analysis.
- **Configurable Event Rate**: Adjustable parameters for event throughput and fraud simulation.

## Prerequisites

- Docker
- Docker Compose

## Getting Started

### 1. Clone the Repository

### 2. Running in Development

Use the development compose file for hot-reloading and debugging:

```bash
docker-compose -f docker-compose-dev.yaml up --build
```

### 3. Running in Production

The main compose file uses the built pulse image, and has configuration fit for production environment, such as lower log level (It is better to use environment variables for such cases, but currently we go with this approach):

```bash
docker-compose up -d --build
```

### 4. Accessing the Services

- **Kafdrop**: Open [http://localhost:9000](http://localhost:9000) to monitor Kafka topics.
- **Kafka Broker**: Available at `localhost:9092`.
- **Event Producer Logs**: View logs with:
  ```bash
  docker-compose logs -f darooghe-pulse
  ```

## Configurable Parameters

Set the following environment variables to control event production:

| Parameter         | Description                                   | Default         |
| ----------------- | --------------------------------------------- | --------------- |
| `EVENT_RATE`      | Average events per minute                     | 100             |
| `PEAK_FACTOR`     | Multiplier during peak hours                  | 2.5             |
| `FRAUD_RATE`      | Fraction of fraudulent transactions           | 0.02            |
| `DECLINED_RATE`   | Fraction of declined transactions             | 0.05            |
| `MERCHANT_COUNT`  | Number of unique merchants                    | 50              |
| `CUSTOMER_COUNT`  | Number of unique customers                    | 1000            |
| `KAFKA_BROKER`    | Kafka broker address                          | kafka:9092      |
| `EVENT_INIT_MODE` | Initialization mode (flush, append, skip)     | flush           |
| `LOG_LEVEL`       | Logging verbosity (DEBUG, INFO, etc.)         | INFO            |

