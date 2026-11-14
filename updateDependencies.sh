#!/bin/bash

./gradlew --refresh-dependencies dependencies mongo-java-server-{core,test-common,{h2,memory,postgresql}-backend,examples}:dependencies --update-locks '*:*'
