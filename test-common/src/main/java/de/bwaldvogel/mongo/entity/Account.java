package de.bwaldvogel.mongo.entity;

import java.math.BigDecimal;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;

@Document
@TypeAlias("account")
public class Account {

    @Id
    private ObjectId id;

    private BigDecimal total;

    protected Account() {
    }

    public Account(BigDecimal total) {
        this.total = total;
    }


    public BigDecimal getTotal() {
        return total;
    }
}