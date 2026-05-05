package com.prechecker.config;

import com.prechecker.domain.MetadataExportV2;
import com.prechecker.service.MetadataLoader;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class EnricherConfigProperties {
    @Bean
    @ConfigurationProperties(prefix = "enricher.search-service")
    public SearchServiceClientProperties searchServiceClientProperties() {
        return new SearchServiceClientProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "enricher.metadata")
    public MetadataProperties metadataProperties() {
        return new MetadataProperties();
    }

    @Bean
    public RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5000);
        factory.setReadTimeout(10000);
        return new RestTemplate(factory);
    }

    @Bean
    public MetadataExportV2 metadataExportV2(MetadataLoader metadataLoader) {
        return metadataLoader.load();
    }
}
