## MongoDB Java Server ##

Stub implementation of the core mongodb server using Java and Netty

### Ideas ###

Allow implementation of different backends. An in-memory backend should be
default such that mongo-java-server can easily be used in unit tests.
Another backend could proxy a real database instance.

Important when testing client code is to check the behavior in case of an
error. One backend could be used to frequently timeout or let queries fail.

Fuzzing the wire protocol could be used to check the robustness of client drivers.

### Similar Projects ###

* [jmockmongo][1] shares the basic idea but focuses on unit testing

[1]: https://github.com/thiloplanz/jmockmongo
