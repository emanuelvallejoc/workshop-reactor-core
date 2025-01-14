package com.example.demo;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface PersonRepository extends ReactiveMongoRepository<Person, String> {
    Mono<Person> findByName(String name);

    Mono<Person> findByNameAndId(String name, String id);
}
