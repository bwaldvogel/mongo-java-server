## MongoDB Java Server ##

Stub implementation of the core mongodb server using Java and Netty

### Ideas ###

Allow implementation of different backends. An in-memory backend is the
default such that mongo-java-server can easily be used in unit tests.
Another backend could proxy a real database instance.

Important when testing client code is to check the behavior in case of an
error. One backend could be used to frequently timeout or let queries fail.

Fuzzing the wire protocol could be used to check the robustness of client drivers.

### Example ###

	MongoServer server = new MongoServer( new MemoryBackend() );
	// bind on a random local port
	InetSocketAddress serverAddress = server.bind();

	MongoClient client = new MongoClient( new ServerAddress( serverAddress ) );

	DBCollection collection = client.getDB( "testdb" ).getCollection( "testcollection" );
	// creates the database and collection in memory and inserts the object
	collection.insert( new BasicDBObject( "key" , "value" ) );

	client.close();
	server.shutdown();

See [examples/][1] for more examples.

### Similar Projects ###

* [jmockmongo][2] shares the basic idea but focuses on unit testing

[1]: mongo-java-server/examples/
[2]: https://github.com/thiloplanz/jmockmongo
