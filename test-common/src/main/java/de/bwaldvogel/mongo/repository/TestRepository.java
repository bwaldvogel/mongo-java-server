package de.bwaldvogel.mongo.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import de.bwaldvogel.mongo.entity.TestEntity;

public interface TestRepository extends MongoRepository<TestEntity, String> {
}