package com.file.s3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class BootLauncher {

    public static void main(String[] args) {
        SpringApplication.run(BootLauncher.class, args);
    }

}
