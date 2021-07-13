package com.example.elasticsearch.controller;

import com.example.elasticsearch.service.ElasticSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ElasticSearchController {

    @Autowired
    private ElasticSearchService elasticSearchService;

    @RequestMapping("/index/put")
    public String putIndex(String indexName,String document){
        elasticSearchService.indexDocument(indexName,document);
        return "success......";
    }


    @RequestMapping("/index/get")
    public String getIndexDocument(String indexName, String document){
        elasticSearchService.getIndexDocument(indexName,document);
        return "get index success.......";
    }

    @RequestMapping("/index/getAsync")
    public String getIndexDocumentAsync(String indexName, String document){
        elasticSearchService.getIndexDocumentAsync(indexName,document);
        return "get index async success.......";
    }

    @RequestMapping("/index/getSource")
    public String getSourceRequest(String indexName, String document){
        elasticSearchService.getSourceRequest(indexName,document);
        return "get source success.........";
    }
}
