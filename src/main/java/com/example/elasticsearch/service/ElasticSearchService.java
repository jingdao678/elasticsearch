package com.example.elasticsearch.service;

import java.io.IOException;

public interface ElasticSearchService {

    void initEs();
    void closeEs() throws IOException;
    void indexDocument(String indexName,String document);
    void getIndexDocument(String indexName,String document);
    void getIndexDocumentAsync(String indexName,String document);
}
