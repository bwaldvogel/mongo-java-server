package de.bwaldvogel.mongo.repository;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import de.bwaldvogel.mongo.entity.Account;

public interface AccountRepository extends MongoRepository<Account, ObjectId> {
}
