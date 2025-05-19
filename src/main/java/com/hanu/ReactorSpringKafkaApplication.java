package com.hanu;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.hanu.sec17test.producer}"})
public class ReactorSpringKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReactorSpringKafkaApplication.class, args);
	}

}
