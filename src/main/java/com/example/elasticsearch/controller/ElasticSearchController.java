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
}
