package de.bwaldvogel.mongo.repository;

import org.springframework.data.mongodb.repository.CountQuery;
import org.springframework.data.mongodb.repository.MongoRepository;

import de.bwaldvogel.mongo.entity.TestEntity;

public interface TestRepository extends MongoRepository<TestEntity, String> {

    @CountQuery(value = "{'value.data': '?0'}")
    int countByValueData(String data);

}
