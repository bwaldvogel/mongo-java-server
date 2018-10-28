package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.entity.Account;
import de.bwaldvogel.mongo.entity.Person;
import de.bwaldvogel.mongo.repository.AccountRepository;
import de.bwaldvogel.mongo.repository.PersonRepository;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = AbstractBackendSpringDataTest.TestConfig.class)
public abstract class AbstractBackendSpringDataTest {

    private static final String DATABASE_NAME = "testdb";

    @EnableMongoRepositories("de.bwaldvogel.mongo.repository")
    static class TestConfig {

        @Bean(destroyMethod = "shutdown")
        public MongoServer mongoServer(MongoBackend backend) {
            return new MongoServer(backend);
        }

        @Bean(destroyMethod = "close")
        public MongoClient mongoClient(MongoServer mongoServer) {
            InetSocketAddress serverAddress = mongoServer.bind();
            return new MongoClient(new ServerAddress(serverAddress));
        }

        @Bean
        public MongoDbFactory mongoDbFactory(MongoClient client) throws Exception {
            return new SimpleMongoDbFactory(client, DATABASE_NAME);
        }

        @Bean
        public MongoTemplate mongoTemplate(MongoDbFactory factory) throws Exception {
            return new MongoTemplate(factory);
        }

    }

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private PersonRepository personRepository;

    @Autowired
    private AccountRepository accountRepository;

    @Before
    public void deleteAll() throws Exception {
        accountRepository.deleteAll();
        personRepository.deleteAll();
    }

    @Test
    public void testSaveFindModifyAndUpdate() throws Exception {

        Person billy = personRepository.save(new Person("Billy", 123));
        personRepository.save(new Person("Joe", 456));

        Person savedBilly = personRepository.findOneByName(billy.getName());
        assertThat(savedBilly.getId()).isNotNull();
        assertThat(savedBilly.getName()).isEqualTo(billy.getName());
        assertThat(savedBilly.getSocialSecurityNumber()).isEqualTo(billy.getSocialSecurityNumber());
        assertThat(savedBilly.getAccounts()).isNull();

        // add zero accounts for all
        for (Person person : personRepository.findAll()) {
            person.addAccount(accountRepository.save(new Account(BigDecimal.ZERO)));
            personRepository.save(person);
        }

        savedBilly = personRepository.findOneByName(billy.getName());
        savedBilly.addAccount(accountRepository.save(new Account(new BigDecimal("8915.35"))));
        personRepository.save(savedBilly);

        Person updatedPerson = personRepository.findOneByName(billy.getName());
        assertThat(updatedPerson.getAccounts()).hasSize(2);

        List<String> databaseNames = toArray(mongoClient.listDatabaseNames());
        assertThat(databaseNames).containsOnly(DATABASE_NAME);

        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        List<String> collectionNames = toArray(database.listCollectionNames());
        assertThat(collectionNames).containsOnly("person", "account", "system.indexes");

        assertThat(toArray(personRepository.findAll())).hasSize(2);
        assertThat(personRepository.count()).isEqualTo(2);
        assertThat(accountRepository.count()).isEqualTo(3);
    }

    @Test
    public void testInsertDuplicateThrows() throws Exception {
        personRepository.save(new Person("Billy", 1));
        personRepository.save(new Person("Alice", 2));

        try {
            personRepository.save(new Person("Joe", 1));
            fail("DuplicateKeyException expected");
        } catch (DuplicateKeyException e) {
            assertThat(e.getMessage()).contains("duplicate key error");
        }
    }
}
