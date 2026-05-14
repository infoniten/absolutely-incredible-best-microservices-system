package com.enricher.service.config;

public class SearchServiceClientProperties {
    private String baseUrl;
    private String globalEndpoint;
    private String revisionEndpoint;
    private String parentEndpoint;

    public String baseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String globalEndpoint() {
        return globalEndpoint;
    }

    public void setGlobalEndpoint(String globalEndpoint) {
        this.globalEndpoint = globalEndpoint;
    }

    public String parentEndpoint() {
        return parentEndpoint;
    }

    public void setParentEndpoint(String parentEndpoint) {
        this.parentEndpoint = parentEndpoint;
    }

    public String revisionEndpoint() {
        return revisionEndpoint;
    }

    public void setRevisionEndpoint(String revisionEndpoint) {
        this.revisionEndpoint = revisionEndpoint;
    }
}
