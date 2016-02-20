package de.bwaldvogel.mongo.repository;

import org.bson.types.ObjectId;
import org.springframework.data.repository.PagingAndSortingRepository;

import de.bwaldvogel.mongo.entity.Account;

public interface AccountRepository extends PagingAndSortingRepository<Account, ObjectId> {
}
