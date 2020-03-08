package de.bwaldvogel.mongo.backend.postgresql.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.backend.IndexKey;

public class PostgresUniqueIndexTest {

    @Test
    void testCreateSelectStatement() throws Exception {
        List<IndexKey> keys = new ArrayList<>();
        keys.add(new IndexKey("a", true));
        keys.add(new IndexKey("b", true));
        PostgresUniqueIndex postgresUniqueIndex = new PostgresUniqueIndex(null, "db", "coll", "idx1", keys, false);
        Map<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("a", "some-value");
        keyValues.put("b", null);
        String selectStatement = postgresUniqueIndex.createSelectStatement(keyValues);
        assertThat(selectStatement).isEqualTo("SELECT id FROM \"db\".\"coll\" WHERE data ->> 'a' = ? AND data ->> 'b' IS NULL");
    }

    @Test
    void testCreateSelectStatement_SparseIndex() throws Exception {
        List<IndexKey> keys = new ArrayList<>();
        keys.add(new IndexKey("a", true));
        keys.add(new IndexKey("b", true));
        PostgresUniqueIndex postgresUniqueIndex = new PostgresUniqueIndex(null, "db", "coll", "idx1", keys, true);
        Map<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("a", "some-value");
        keyValues.put("b", null);
        String selectStatement = postgresUniqueIndex.createSelectStatement(keyValues);
        assertThat(selectStatement).isEqualTo("SELECT id FROM \"db\".\"coll\" WHERE data ->> 'a' = ? AND data ->> 'b' = ?");
    }

}
