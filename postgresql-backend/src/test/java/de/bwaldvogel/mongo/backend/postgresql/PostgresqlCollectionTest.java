package de.bwaldvogel.mongo.backend.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;

public class PostgresqlCollectionTest {

    @Test
    public void testConvertOrderByToSql() throws Exception {
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document())).isEqualTo("");
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document("key", 1))).isEqualTo("ORDER BY data ->> 'key' ASC NULLS LAST");
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document("key1", 1).append("key2", -1))).isEqualTo("ORDER BY data ->> 'key1' ASC NULLS LAST, data ->> 'key2' DESC NULLS LAST");
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document("$natural", 1))).isEqualTo("ORDER BY id ASC NULLS LAST");
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document("$natural", -1))).isEqualTo("ORDER BY id DESC NULLS LAST");

        try {
            PostgresqlCollection.convertOrderByToSql(new Document("foo", "bar"));
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Illegal sort value: bar");
        }

        try {
            PostgresqlCollection.convertOrderByToSql(new Document("$foo", 1));
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Illegal key: $foo");
        }
    }

}