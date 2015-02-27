package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractSimpleBackendTest;

public class SimpleMemoryBackendTest extends AbstractSimpleBackendTest {

	@Override
	protected MongoBackend createBackend() throws Exception {
		return new MemoryBackend();
	}

}
