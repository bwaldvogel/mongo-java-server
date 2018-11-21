package de.bwaldvogel.mongo.backend.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;

public class PostgresqlCollectionTest {

    @Test
    public void testConvertOrderByToSql() throws Exception {
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document())).isEqualTo("");
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document("key", 1))).isEqualTo("ORDER BY data ->> 'key' ASC NULLS FIRST");
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document("key1", 1).append("key2", -1))).isEqualTo("ORDER BY data ->> 'key1' ASC NULLS FIRST, data ->> 'key2' DESC NULLS LAST");
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document("$natural", 1))).isEqualTo("ORDER BY id ASC NULLS FIRST");
        assertThat(PostgresqlCollection.convertOrderByToSql(new Document("$natural", -1))).isEqualTo("ORDER BY id DESC NULLS LAST");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> PostgresqlCollection.convertOrderByToSql(new Document("foo", "bar")))
            .withMessage("Illegal sort value: bar");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> PostgresqlCollection.convertOrderByToSql(new Document("$foo", 1)))
            .withMessage("Illegal key: $foo");
    }

}