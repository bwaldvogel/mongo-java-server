#!/bin/bash

./gradlew buildEnvironment dependencies mongo-java-server-{core,test-common,{h2,memory,postgresql}-backend,examples}:dependencies --write-locks
