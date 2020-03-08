package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import java.net.InetSocketAddress;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.entity.Account;
import de.bwaldvogel.mongo.entity.Person;
import de.bwaldvogel.mongo.entity.SubEntity;
import de.bwaldvogel.mongo.entity.TestEntity;
import de.bwaldvogel.mongo.repository.AccountRepository;
import de.bwaldvogel.mongo.repository.PersonRepository;
import de.bwaldvogel.mongo.repository.TestRepository;

@ExtendWith(SpringExtension.class)
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

    @Autowired
    private TestRepository testRepository;

    @BeforeEach
    void deleteAll() throws Exception {
        accountRepository.deleteAll();
        personRepository.deleteAll();
        testRepository.deleteAll();
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

        assertThat(mongoClient.listDatabaseNames()).containsExactly(DATABASE_NAME);

        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        assertThat(database.listCollectionNames()).containsExactlyInAnyOrder("person", "account", "test");

        assertThat(personRepository.findAll()).hasSize(2);
        assertThat(personRepository.count()).isEqualTo(2);
        assertThat(accountRepository.count()).isEqualTo(3);
    }

    @Test
    public void testInsertDuplicateThrows() throws Exception {
        personRepository.save(new Person("Billy", 1));
        personRepository.save(new Person("Alice", 2));

        assertThatExceptionOfType(DuplicateKeyException.class)
            .isThrownBy(() -> personRepository.save(new Person("Joe", 1)))
            .withMessageContaining("duplicate key error");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/39
    @Test
    public void testDeleteWithUniqueIndexes() throws Exception {
        TestEntity document = testRepository.save(new TestEntity("DOC_1", "Text1"));

        // update value of indexed property
        document.setText("Text1 (updated)");
        testRepository.save(document);

        assertThat(testRepository.findAll()).hasSize(1);

        testRepository.deleteById("DOC_1");

        assertThat(testRepository.findAll()).isEmpty();

        testRepository.save(new TestEntity("DOC_1", "Text1"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/66
    @Test
    public void testCountByValueData() throws Exception {
        testRepository.save(new TestEntity("DOC_1", "Text1")
            .withValue(new SubEntity("v1")));

        testRepository.save(new TestEntity("DOC_2", "Text2")
            .withValue(new SubEntity("v1")));

        testRepository.save(new TestEntity("DOC_3", "Text3")
            .withValue(new SubEntity("v2")));

        assertThat(testRepository.countByValueData("v1")).isEqualTo(2);
        assertThat(testRepository.countByValueData("v2")).isEqualTo(1);
        assertThat(testRepository.countByValueData("v3")).isEqualTo(0);
    }

}
