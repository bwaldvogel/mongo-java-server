package de.bwaldvogel.mongo.backend.memory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractBackendSpringDataTest;
import de.bwaldvogel.mongo.backend.h2.H2Backend;

@ContextConfiguration(classes = H2OnDiskBackendSpringDataTest.TestConfig.class)
public class H2OnDiskBackendSpringDataTest extends AbstractBackendSpringDataTest {

    static class TemporaryFileH2Backend extends H2Backend {

        private Path temporaryDirectory;

        public TemporaryFileH2Backend() throws IOException {
            this(createTempFile());
        }

        private static Path createTempFile() throws IOException {
            return Files.createTempFile(H2OnDiskBackendSpringDataTest.class.getSimpleName(), ".mv");
        }

        private TemporaryFileH2Backend(Path temporaryDirectory) {
            super(temporaryDirectory.toString());
            this.temporaryDirectory = temporaryDirectory;
        }

        @Override
        public void close() {
            super.close();
            if (temporaryDirectory != null) {
                try {
                    Files.delete(temporaryDirectory);
                    temporaryDirectory = null;
                } catch (IOException e) {
                    throw new RuntimeException("Failed to delete " + temporaryDirectory, e);
                }
            }
        }

    }

    static class TestConfig {

        @Bean
        MongoBackend backend() throws Exception {
            return new TemporaryFileH2Backend();
        }

    }


}