package de.bwaldvogel.mongo.repository;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import de.bwaldvogel.mongo.entity.Person;

public interface PersonRepository extends MongoRepository<Person, ObjectId> {

    Person findOneByName(String name);
}
