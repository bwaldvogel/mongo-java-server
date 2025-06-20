[![CI](https://github.com/bwaldvogel/mongo-java-server/workflows/CI/badge.svg)](https://github.com/bwaldvogel/mongo-java-server/actions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.bwaldvogel/mongo-java-server/badge.svg)](http://maven-badges.herokuapp.com/maven-central/de.bwaldvogel/mongo-java-server)
[![codecov](https://codecov.io/gh/bwaldvogel/mongo-java-server/branch/main/graph/badge.svg?token=jZ7zBT4niu)](https://codecov.io/gh/bwaldvogel/mongo-java-server)
[![BSD 3-Clause License](https://img.shields.io/github/license/bwaldvogel/mongo-java-server.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://www.paypal.me/BenediktWaldvogel)

# MongoDB Java Server #

Fake implementation of the core [MongoDB][mongodb] server in Java that can be used for integration tests.

Think of H2/HSQLDB/SQLite but for MongoDB.

The [MongoDB Wire Protocol][wire-protocol] is implemented with [Netty][netty].
Different backends are possible and can be extended.

## In-Memory backend ##

The in-memory backend is the default backend that is typically used to fake MongoDB for integration tests.
It supports most CRUD operations, commands and the aggregation framework.
Some features are not yet implemented, such as transactions, full-text search or map/reduce.

Add the following Maven dependency to your project:

```xml
<dependency>
    <groupId>de.bwaldvogel</groupId>
    <artifactId>mongo-java-server</artifactId>
    <version>1.47.0</version>
</dependency>
```

### Example ###

```java
class SimpleTest {

    private MongoCollection<Document> collection;
    private MongoClient client;
    private MongoServer server;

    @BeforeEach
    void setUp() {
        server = new MongoServer(new MemoryBackend());

        // optionally:
        // server.enableSsl(key, keyPassword, certificate);
        // server.enableOplog();

        // bind on a random local port
        String connectionString = server.bindAndGetConnectionString();

        client = MongoClients.create(connectionString);
        collection = client.getDatabase("testdb").getCollection("testcollection");
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.shutdown();
    }

    @Test
    void testSimpleInsertQuery() throws Exception {
        assertThat(collection.countDocuments()).isZero();

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 1).append("key", "value");
        collection.insertOne(obj);

        assertThat(collection.countDocuments()).isEqualTo(1L);
        assertThat(collection.find().first()).isEqualTo(obj);
    }

}
```

### Example with SpringBoot ###

```java
@RunWith(SpringRunner.class)
@SpringBootTest(classes={SimpleSpringBootTest.TestConfiguration.class})
public class SimpleSpringBootTest {

    @Autowired private MyRepository repository;

    @Before
    public void setUp() {
        // initialize your repository with some test data
        repository.deleteAll();
        repository.save(...);
    }

    @Test
    public void testMyRepository() {
        // test your repository ...
        ...
    }

    @Configuration
    @EnableMongoTestServer
    @EnableMongoRepositories(basePackageClasses={MyRepository.class})
    protected static class TestConfiguration {
        // test bean definitions ...
        ...
    }
}

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(MongoTestServerConfiguration.class)
public @interface EnableMongoTestServer {

}

public class MongoTestServerConfiguration {
	@Bean
	public MongoTemplate mongoTemplate(MongoDatabaseFactory mongoDbFactory) {
		return new MongoTemplate(mongoDbFactory);
	}

	@Bean
	public MongoDatabaseFactory mongoDbFactory(MongoServer mongoServer) {
		String connectionString = mongoServer.getConnectionString();
		return new SimpleMongoClientDatabaseFactory(connectionString + "/test");
	}

	@Bean(destroyMethod = "shutdown")
	public MongoServer mongoServer() {
		MongoServer mongoServer = new MongoServer(new MemoryBackend());
		mongoServer.bind();
		return mongoServer;
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
    <version>1.47.0</version>
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

The PostgreSQL backend is a proof-of-concept implementation that connects the server to a database in a running
PostgreSQL 9.5+ instance. Each MongoDB database is mapped to a schema in
Postgres and each MongoDB collection is stored as a table.

```xml
<dependency>
    <groupId>de.bwaldvogel</groupId>
    <artifactId>mongo-java-server-postgresql-backend</artifactId>
    <version>1.47.0</version>
</dependency>
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
## Building a "fat" JAR that contains all dependencies ##

If you want to build a version that is not on Maven Central you can do the following:

1. Build a "fat" JAR that includes all dependencies using "`./gradlew shadowJar`"
2. Copy `build/libs/mongo-java-server-[version]-all.jar` to your project, e.g. to the `libs` directory.
3. Import that folder (e.g. via Gradle using `testCompile fileTree(dir: 'libs', include: '*.jar')`)

## Contributing ##

Please read the [contributing guidelines](CONTRIBUTING.md) if you want to contribute code to the project.

If you want to thank the author for this library or want to support the maintenance work, we are happy to receive a donation.

[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://www.paypal.me/BenediktWaldvogel)

## Ideas for other backends ##

### Faulty backend ###

A faulty backend could randomly fail queries or cause timeouts. This could be
used to test the client for error resilience.

### Fuzzy backend ###

Fuzzing the wire protocol could be used to check the robustness of client
drivers.

## Transactions ##

Please note that transactions are currently not supported.
Please see [the discussion in issue #143](https://github.com/bwaldvogel/mongo-java-server/issues/143).

When using `mongo-java-server` for integration tests, you can use [Testcontainers][testcontainers] or [Embedded MongoDB][embedded-mongodb] instead to spin-up a real MongoDB that will have full transaction support.

## Related Work ##

* [Testcontainers][testcontainers]
  * Can be used to spin-up a real MongoDB instance in a Docker container

* [Embedded MongoDB][embedded-mongodb]
  * Spins up a real MongoDB instance

* [fongo][fongo]
  * focus on unit testing
  * no wire protocol implementation
  * intercepts the java mongo driver
  * currently used in [nosql-unit][nosql-unit]

[mongodb]: http://www.mongodb.org/
[wire-protocol]: https://docs.mongodb.org/manual/reference/mongodb-wire-protocol/
[netty]: http://netty.io/
[testcontainers]: https://www.testcontainers.org/
[embedded-mongodb]: https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo
[fongo]: https://github.com/fakemongo/fongo
[nosql-unit]: https://github.com/lordofthejars/nosql-unit
[h2-mvstore]: http://www.h2database.com/html/mvstore.html
