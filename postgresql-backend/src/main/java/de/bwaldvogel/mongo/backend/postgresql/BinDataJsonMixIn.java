package de.bwaldvogel.mongo.backend.postgresql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

abstract class BinDataJsonMixIn {

    @JsonCreator
    BinDataJsonMixIn(@JsonProperty("data") byte[] data) {}

}
