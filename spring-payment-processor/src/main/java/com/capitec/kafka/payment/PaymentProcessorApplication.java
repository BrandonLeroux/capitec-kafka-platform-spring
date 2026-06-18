package com.capitec.kafka.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class PaymentProcessorApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentProcessorApplication.class, args);
    }
}
