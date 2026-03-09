#!/bin/bash

if [ "$1" = "-c" ]; then
    if [ ! -d compiled ]; then
        mkdir compiled
        mkdir compiled/UserService
        mkdir compiled/ProductService
        mkdir compiled/OrderService
        mkdir compiled/ISCS
        mkdir compiled/Parser
    fi
    javac -d compiled/UserService src/UserService/UserService.java
    javac -d compiled/ProductService src/ProductService/ProductService.java
    javac -d compiled/OrderService src/OrderService/OrderService.java
    javac -d compiled/ISCS src/ISCS/ISCS.java
    javac -d compiled/Parser src/Parser/Parser.java
fi

if [ "$1" = "-u" ]; then
    java -cp compiled/UserService/ UserService config.json
fi

if [ "$1" = "-p" ]; then
    java -cp compiled/ProductService/ ProductService config.json
fi

if [ "$1" = "-i" ]; then
    java -cp compiled/ISCS/ ISCS config.json
fi

if [ "$1" = "-o" ]; then
    java -cp compiled/OrderService/ OrderService config.json
fi

if [ "$1" = "-w" ]; then
    java -cp compiled/Parser/ Parser config.json "$2"
fi