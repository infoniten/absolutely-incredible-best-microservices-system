package com.enricher.service.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@Slf4j
public class CorsConfig {

    @Bean
    public WebMvcConfigurer webMvcConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**")
                        .allowedOriginPatterns(
                                "https://*sigma.sbrf.ru",
                                "https://*delta.sbrf.ru",
                                "http://localhost:5173",
                                "https://simulation-ui.apps.a8uaat10.k8s.delta.sbrf.ru"
                        )
                        .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD")
                        .allowedHeaders("*")
                        .exposedHeaders(
                                "Authorization",
                                "Content-Type",
                                "Content-Disposition",
                                "Allow-Control-Allow-Origin",
                                "Allow-Control-Allow-Credentials"
                        )
                        .allowCredentials(false)
                        .maxAge(3600);
            }
        };
    }
}
