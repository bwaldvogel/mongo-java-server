package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

public class TestUtils {

    private TestUtils() {
    }

    public static <T> List<T> toArray(Iterable<T> iterable) {
        List<T> array = new ArrayList<T>();
        for (T obj : iterable) {
            array.add(obj);
        }
        return array;
    }

    public static Document json(String string) {
        string = string.trim();
        if (!string.startsWith("{")) {
            string = "{" + string + "}";
        }
        return Document.parse(string);
    }
}
