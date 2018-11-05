[![Build Status](https://travis-ci.org/bwaldvogel/mongo-java-server.png?branch=master)](https://travis-ci.org/bwaldvogel/mongo-java-server)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.bwaldvogel/mongo-java-server/badge.svg)](http://maven-badges.herokuapp.com/maven-central/de.bwaldvogel/mongo-java-server)
[![Coverage Status](https://coveralls.io/repos/github/bwaldvogel/mongo-java-server/badge.svg?branch=master)](https://coveralls.io/github/bwaldvogel/mongo-java-server?branch=master)
[![BSD 3-Clause License](https://img.shields.io/github/license/bwaldvogel/mongo-java-server.svg)](https://opensource.org/licenses/BSD-3-Clause)

# MongoDB Java Server #

Stub implementation of the core [MongoDB][mongodb] server in Java.
The [MongoDB Wire Protocol][wire-protocol] is implemented with [Netty][netty].
Different backends are possible and can be easily extended.

## Usage
Add the following Maven dependency to your project:

```xml
<dependency>
    <groupId>de.bwaldvogel</groupId>
    <artifactId>mongo-java-server</artifactId>
    <version>1.9.5</version>
</dependency>
```

## In-Memory backend ##

The in-memory backend is the default, such that mongo-java-server can be used
as stub in unit tests. It supports the basic CRUD operations.
However, not all features are implemented, such as full-text search or map/reduce.

### Example ###

```java
public class SimpleTest {

    private MongoCollection<Document> collection;
    private MongoClient client;
    private MongoServer server;

    @Before
    public void setUp() {
        server = new MongoServer(new MemoryBackend());

        // bind on a random local port
        InetSocketAddress serverAddress = server.bind();

        client = new MongoClient(new ServerAddress(serverAddress));
        collection = client.getDatabase("testdb").getCollection("testcollection");
    }

    @After
    public void tearDown() {
        client.close();
        server.shutdown();
    }

    @Test
    public void testSimpleInsertQuery() throws Exception {
        assertEquals(0, collection.count());

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 1).append("key", "value");
        collection.insertOne(obj);

        assertEquals(1, collection.count());
        assertEquals(obj, collection.find().first());
    }

}
```

## H2 MVStore backend ##

The [H2 MVStore][h2-mvstore] backend connects the server to a `MVStore` that
can either be in-memory or on-disk.

```xml
<dependency>
    <groupId>de.bwaldvogel</groupId>
    <artifactId>mongo-java-server-h2-backend</artifactId>
    <version>1.9.5</version>
</dependency>
```

### Example ###

```java
public class Application {

    public static void main(String[] args) throws Exception {
        MongoServer server = new MongoServer(new H2Backend("database.mv"));
        server.bind("localhost", 27017);
    }

}
```

## PostgreSQL backend ##

The PostgreSQL backend connects the server to a database in a running
PostgreSQL 9.5+ instance. Each MongoDB database is mapped to a schema in
Postgres and each MongoDB collection is stored as a table.

```xml
<dependency>
    <groupId>de.bwaldvogel</groupId>
    <artifactId>mongo-java-server-postgresql-backend</artifactId>
    <version>1.9.5</version>
</dependency>
```

For integration tests, a PostgreSQL instance can be created in a docker container:
```
docker run --name postgres-mongo-java-server-test -p 5432:5432 -e POSTGRES_USER=mongo-java-server-test -e POSTGRES_PASSWORD=mongo-java-server-test -e POSTGRES_DB=mongo-java-server-test -d postgres:9.6-alpine
```

### Example ###

```java
public class Application {

    public static void main(String[] args) throws Exception {
        DataSource dataSource = new org.postgresql.jdbc3.Jdbc3PoolingDataSource();
        dataSource.setDatabaseName(…);
        dataSource.setUser(…);
        dataSource.setPassword(…);
        MongoServer server = new MongoServer(new PostgresqlBackend(dataSource));
        server.bind("localhost", 27017);
    }

}
```

## Ideas for other backends ##

### Faulty backend ###

A faulty backend could randomly fail queries or cause timeouts. This could be
used to test the client for error resilience.

### Fuzzy backend ###

Fuzzing the wire protocol could be used to check the robustness of client
drivers.

## Related Work ##

* [jmockmongo][jmockmongo]
    * shares the basic idea of implementing the wire protocol with Netty
    * focus on in-memory backend for unit testing

* [fongo][fongo]
    * focus on unit testing
    * no wire protocol implementation
    * intercepts the java mongo driver
    * currently used in [nosql-unit][nosql-unit]

[mongodb]: http://www.mongodb.org/
[wire-protocol]: https://docs.mongodb.org/manual/reference/mongodb-wire-protocol/
[netty]: http://netty.io/
[jmockmongo]: https://github.com/thiloplanz/jmockmongo
[fongo]: https://github.com/fakemongo/fongo
[nosql-unit]: https://github.com/lordofthejars/nosql-unit
[h2-mvstore]: http://www.h2database.com/html/mvstore.html
