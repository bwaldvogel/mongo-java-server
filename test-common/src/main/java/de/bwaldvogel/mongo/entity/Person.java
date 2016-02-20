package de.bwaldvogel.mongo.entity;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document
@TypeAlias("person")
public class Person {

    @Id
    private ObjectId id;

    private String name;

    @Indexed(unique = true)
    @Field("ssn")
    private int socialSecurityNumber;

    @DBRef
    private List<Account> accounts;

    protected Person() {
    }

    public Person(String name, int socialSecurityNumber) {
        this.name = name;
        this.socialSecurityNumber = socialSecurityNumber;
    }

    public ObjectId getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getSocialSecurityNumber() {
        return socialSecurityNumber;
    }

    public List<Account> getAccounts() {
        return accounts;
    }

    public void addAccount(Account account) {
        if (accounts == null) {
            accounts = new ArrayList<>();
        }
        accounts.add(account);
    }
}