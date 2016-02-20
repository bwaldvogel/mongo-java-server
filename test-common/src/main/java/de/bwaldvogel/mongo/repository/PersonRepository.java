package de.bwaldvogel.mongo.repository;

import org.bson.types.ObjectId;
import org.springframework.data.repository.PagingAndSortingRepository;

import de.bwaldvogel.mongo.entity.Person;

public interface PersonRepository extends PagingAndSortingRepository<Person, ObjectId> {

    Person findOneByName(String name);
}
