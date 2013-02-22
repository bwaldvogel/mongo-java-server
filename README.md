[![Build Status](https://travis-ci.org/bwaldvogel/mongo-java-server.png?branch=master)](https://travis-ci.org/bwaldvogel/mongo-java-server)

## MongoDB Java Server ##

Implementation of the core [MongoDB][mongodb] server using Java with different
possible backends. The [MongoDB Wire Protocol][wire-protocol] is implemented
using [Netty][netty].

### In-Memory backend ###

The in-memory backend is the default, such that mongo-java-server can be used
as stub in unit tests. It doesn't support all features of the original MongoDB,
and probably never will.

### Ideas for other backends ###

#### Faulty backend ####

A faulty backend could randomly fail queries or cause timeouts. This could be
used to test the client for error resilience.

#### Fuzzy backend ####

Fuzzing the wire protocol could be used to check the robustness of client
drivers.

### Unit test example ###

	MongoServer server = new MongoServer( new MemoryBackend() );
	// bind on a random local port
	InetSocketAddress serverAddress = server.bind();

	MongoClient client = new MongoClient( new ServerAddress( serverAddress ) );

	DBCollection collection = client.getDB( "testdb" ).getCollection( "testcollection" );
	// creates the database and collection in memory and inserts the object
	collection.insert( new BasicDBObject( "key" , "value" ) );

	assertEquals( 1, collection.count() );

	assertEquals( "value", collection.find().toArray().get( 0 ).get( "key" ) );

	client.close();
	server.shutdown();

### Similar Projects ###

* [jmockmongo][jmockmongo]
	* shares the basic idea but focuses on unit testing

* [jongo][jongo]
	* similar to jmockmongo – focus on unit testing
	* currently used in [nosql-unit][nosql-unit]

[mongodb]: http://www.mongodb.org/
[wire-protocol]: http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol
[netty]: https://netty.io/
[jmockmongo]: https://github.com/thiloplanz/jmockmongo/
[jongo]: https://github.com/foursquare/fongo/
[nosql-unit]: https://github.com/lordofthejars/nosql-unit/
