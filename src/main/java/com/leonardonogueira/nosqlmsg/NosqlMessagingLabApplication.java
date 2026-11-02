package com.leonardonogueira.nosqlmsg;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class NosqlMessagingLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(NosqlMessagingLabApplication.class, args);
    }
}

