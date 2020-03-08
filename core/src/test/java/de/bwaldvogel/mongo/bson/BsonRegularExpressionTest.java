package de.bwaldvogel.mongo.bson;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

public class BsonRegularExpressionTest {

    @Test
    void testToString() throws Exception {
        assertThat(new BsonRegularExpression(".*")).hasToString("{\"$regex\" : \".*\"}");
        assertThat(new BsonRegularExpression(".*", "im")).hasToString("{\"$regex\" : \".*\", \"$options\" : \"im\"}");
    }

    @Test
    void testConvertToRegularExpressionWithoutOptions() throws Exception {
        BsonRegularExpression regularExpression = BsonRegularExpression.convertToRegularExpression(json("$regex: 'a.*z'"));
        assertThat(regularExpression.getPattern()).isEqualTo("a.*z");
        assertThat(regularExpression.getOptions()).isEqualTo("");
        Pattern pattern = regularExpression.createPattern();
        assertThat(pattern.matcher("abcz").matches()).isTrue();
        assertThat(pattern.matcher("AbcZ").matches()).isFalse();
        assertThat(pattern.flags()).isEqualTo(Pattern.UNICODE_CASE);
    }

    @Test
    void testConvertToRegularExpressionWithOptions() throws Exception {
        BsonRegularExpression regularExpression = BsonRegularExpression.convertToRegularExpression(new Document("$regex", "a.*z").append("$options", "usxmi"));
        assertThat(regularExpression.getPattern()).isEqualTo("a.*z");
        assertThat(regularExpression.getOptions()).isEqualTo("usxmi");
        Pattern pattern = regularExpression.createPattern();
        assertThat(pattern.flags()).isEqualTo(
            Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE | Pattern.COMMENTS | Pattern.MULTILINE | Pattern.DOTALL
        );
    }

    @Test
    void testConvertToRegularExpressionWithIllegalArgument() throws Exception {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> BsonRegularExpression.convertToRegularExpression("abc"))
            .withMessage("'abc' is not a regular expression");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> BsonRegularExpression.convertToRegularExpression(new Document()))
            .withMessage("'{}' is not a regular expression");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> BsonRegularExpression.convertToRegularExpression(new Document("foo", "bar")))
            .withMessage("'{\"foo\" : \"bar\"}' is not a regular expression");
    }

}
