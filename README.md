# Epoxy Implementation

This project implements the Epoxy system as described in the VLDB 2023 paper. Epoxy is a distributed transaction coordination system designed to provide strong consistency and high performance across multiple data stores.


## Setup Instructions

### Prerequisites

- Docker Desktop

### Building the Project

1. **Clone the repository**:
    ```sh
    git clone https://github.com/aksharaganapathi/CS6360-Project-Group-11.git
    cd epoxy-implementation
    ```

### Running the Project with Docker

1. **Ensure Docker Desktop is running**.

2. **Build and start the Docker containers in a terminal running as administrator in the project directory**:
    ```sh
    docker-compose up --build
    ```

3. **Verify the containers are running**:
    ```sh
    docker-compose ps
    ```

### Running Benchmarks

The project includes several benchmarks to evaluate the performance of the Epoxy system.

1. **Run the benchmarks**:
    - Open the `src/main/java/org/example/EpoxyRunner.java` file.
    - Run the `EpoxyRunner` class.

## Project Components

### Docker Compose Services

- **Postgres**: PostgreSQL database for transactional data.
- **MongoDB**: MongoDB database for document data.
- **Elasticsearch**: Elasticsearch for search and analytics.
- **App**: The main application container running the Epoxy system.

### Java Classes

- **EpoxyCoordinator**: Manages transactions and coordinates between different data stores.
- **TransactionContext**: Represents the context of a transaction.
- **DataStoreShim**: Interface for data store operations.
- **PostgresShim**: Implementation of `DataStoreShim` for PostgreSQL.
- **MongoDBShim**: Implementation of `DataStoreShim` for MongoDB.
- **ElasticsearchShim**: Implementation of `DataStoreShim` for Elasticsearch.
- **Benchmarks**: Various benchmark classes to evaluate system performance.

## References

- [Epoxy: Consistent, Distributed Transactions for Multi-Store Systems](https://vldb.org/pvldb/vol16/p1234-epoxy.pdf) - VLDB 2023 Paper