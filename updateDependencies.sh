#!/bin/bash

./gradlew --refresh-dependencies mongo-java-server-{core,test-common,{h2,memory,postgres}-backend}:dependencies --update-locks '*:*'
