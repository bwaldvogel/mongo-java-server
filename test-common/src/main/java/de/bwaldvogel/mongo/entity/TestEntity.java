package de.bwaldvogel.mongo.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "test")
public class TestEntity {

    @Id
    private String id;

    @Indexed(unique = true)
    private String text;

    private SubEntity value;

    public TestEntity() {
    }

    public TestEntity(String id, String text) {
        this.id = id;
        this.text = text;
    }

    public TestEntity withValue(SubEntity value) {
        setValue(value);
        return this;
    }

    public String getId() {
        return id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public SubEntity getValue() {
        return value;
    }

    public void setValue(SubEntity value) {
        this.value = value;
    }
}
