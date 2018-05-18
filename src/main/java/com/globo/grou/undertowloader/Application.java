package com.globo.grou.undertowloader;

import com.globo.grou.undertowloader.services.LoaderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.stereotype.Component;

@SpringBootApplication
@EnableAutoConfiguration(exclude = {WebMvcAutoConfiguration.class })
public class Application {

    @Component
    public class Runner {

        @Autowired
        public Runner(LoaderService loaderService) {
            loaderService.run();
        }
    }

    public static void main(String ...args) {
        SpringApplication.run(Application.class, args);
    }
}
