package de.bwaldvogel.mongo.backend.memory;

import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendSpringDataTest;

@ContextConfiguration(classes = MemoryBackendSpringDataTest.TestConfig.class)
public class MemoryBackendSpringDataTest extends AbstractBackendSpringDataTest {

    static class TestConfig {

        @Bean
        MongoBackend backend() {
            return new MemoryBackend();
        }

    }

}