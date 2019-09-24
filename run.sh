#!/bin/sh
javac -cp .:/home/ubuntu/build-target/common-utils/tools-0.001-SNAPSHOT.jar EsSearch.java
java -cp .:/home/ubuntu/build-target/common-utils/tools-0.001-SNAPSHOT.jar EsSearch
