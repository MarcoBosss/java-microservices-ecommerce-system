#!/bin/bash

GSON="lib/gson-2.13.2.jar"
PG="lib/postgresql-42.7.10.jar"

if [ "$1" = "-c" ]; then
    if [ ! -d compiled ]; then
        mkdir compiled
        mkdir compiled/UserService
        mkdir compiled/ProductService
        mkdir compiled/OrderService
        mkdir compiled/ISCS
        mkdir compiled/Parser
    fi
    javac -cp "$PG:$GSON" -d compiled/UserService src/common/*.java src/UserService/UserService.java
    javac -cp "$PG:$GSON" -d compiled/ProductService src/common/*.java src/ProductService/ProductService.java
    javac -cp "$PG:$GSON" -d compiled/OrderService src/common/*.java src/OrderService/OrderService.java
    javac -d compiled/ISCS src/common/*.java src/ISCS/ISCS.java
    javac -d compiled/Parser src/common/*.java src/Parser/Parser.java
    javac -cp "$GSON" -d compiled src/common/*.java src/LoadBalancer.java
fi

if [ "$1" = "-u" ]; then
    java -cp "compiled/UserService:$PG:$GSON" UserService config.json
fi

if [ "$1" = "-p" ]; then
    java -cp "compiled/ProductService:$PG:$GSON" ProductService config.json
fi

if [ "$1" = "-i" ]; then
    java -cp compiled/ISCS/ ISCS config.json
fi

if [ "$1" = "-o" ]; then
    java -cp "compiled/OrderService:$PG:$GSON" OrderService config.json
fi

if [ "$1" = "-l" ]; then
    java -cp "compiled:$PG:$GSON" LoadBalancer config.json
fi

if [ "$1" = "-w" ]; then
    java -cp compiled/Parser/ Parser config.json "$2"
fi