package de.bwaldvogel.mongo.backend.memory;

import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendSpringDataTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

@ContextConfiguration(classes = H2BackendSpringDataTest.TestConfig.class)
class H2BackendSpringDataTest extends AbstractBackendSpringDataTest {

    static class TestConfig {

        @Bean
        MongoBackend backend() {
            return H2Backend.inMemory();
        }

    }

}
