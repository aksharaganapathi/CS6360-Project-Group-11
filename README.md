# Epoxy Implementation

This project implements the Epoxy system as described in the VLDB 2023 paper. Epoxy is a distributed transaction coordination system designed to provide strong consistency and high performance across multiple data stores.


## Setup Instructions

### Prerequisites

- Docker Desktop
- Any JDK 17+ must be in use by your system

### Building the Project

1. **Clone the repository**:
    ```sh
    git clone https://github.com/aksharaganapathi/CS6360-Project-Group-11.git
    cd CS6360-Project-Group-11
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

    You should see something like this where all containers are created and up:
   ```
    CONTAINER ID   IMAGE                                                   COMMAND                  CREATED             STATUS          PORTS                               NAMES
    785c329bd120   mongo:4.4                                               "docker-entrypoint.s…"   About an hour ago   Up 55 minutes   0.0.0.0:27017->27017/tcp            epoxy-implementation-mongo-1
    3855c816d0ac   postgres:13                                             "docker-entrypoint.s…"   5 days ago          Up 55 minutes   0.0.0.0:5432->5432/tcp              epoxy-implementation-postgres-1
    c12157333ccc   mysql:8.0                                               "docker-entrypoint.s…"   5 days ago          Up 55 minutes   0.0.0.0:3306->3306/tcp, 33060/tcp   epoxy-implementation-mysql-1
    22d6c8f7e681   docker.elastic.co/elasticsearch/elasticsearch:7.17.12   "/bin/tini -- /usr/l…"   5 days ago          Up 55 minutes   0.0.0.0:9200->9200/tcp, 9300/tcp    epoxy-implementation-elasticsearch-1
   ```

### Running Benchmarks

Once your docker images are running, you can run the benchmarks. The project includes several benchmarks to evaluate the performance of the Epoxy system. To simplify the process, the project has been compiled into a JAR with all its dependencies, so you don't have to build with Maven yourself. 

1. **Run the benchmarks**:
    - In a terminal running the project directory, run the following command:
    - ```java -jar epoxy-implementation-1.0-SNAPSHOT-jar-with-dependencies.jar```

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
