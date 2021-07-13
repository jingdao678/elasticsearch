package com.example.elasticsearch.service.impl;

import com.example.elasticsearch.service.ElasticSearchService;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

@Service
@Slf4j
public class ElasticSearchServiceImpl implements ElasticSearchService {

    private RestHighLevelClient client;

    @PostConstruct
    @Override
    public void initEs() {
         client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.1.106", 9200, "http")));
                      //  new HttpHost("localhost", 9201, "http")));
        System.err.println("client:"+client);
    }

    @Override
    public void closeEs()  {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void indexDocument(String indexName, String document) {
        IndexRequest indexRequest = new IndexRequest(indexName).id(document).source("user","chq",
                "postDate",new Date(),"message","hello elasticsearch3333");
              //  .setIfSeqNo(9)
                //.setIfPrimaryTerm(2);
        try {
            IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);
            System.err.println("index:"+index);
            ReplicationResponse.ShardInfo shardInfo = index.getShardInfo();
            if(shardInfo.getFailed()>0){
            //    ReplicationResponse.ShardInfo.Failure[] failures = shardInfo.getFailures();
                for(ReplicationResponse.ShardInfo.Failure failures: shardInfo.getFailures()){
                    System.err.println("fail :"+failures.reason());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
     ///   closeEs();
    }

    @Override
    public void getIndexDocument(String indexName, String document){
        GetRequest getRequest = new GetRequest(indexName,document);
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        } catch (IOException  | ElasticsearchStatusException e) {
           e.printStackTrace();
        }
        String index = getResponse.getIndex();
        String id = getResponse.getId();
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
            System.err.println("getindex........");
        } else {
            System.err.println("document not exists!!!");
        }
    }

    @Override
    public void getIndexDocumentAsync(String indexName, String document) {
        GetRequest getRequest = new GetRequest(indexName,document);
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                String index = getResponse.getIndex();
                String id = getResponse.getId();
                if (getResponse.isExists()) {
                    long version = getResponse.getVersion();
                    String sourceAsString = getResponse.getSourceAsString();
                    Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                    byte[] sourceAsBytes = getResponse.getSourceAsBytes();
                    System.err.println("getindex........");
                } else {
                    System.err.println("document not exists!!!");
                }
            }

            @Override
            public void onFailure(Exception e) {
                    System.err.println("exception .............");
                    e.printStackTrace();
            }
        };

        client.getAsync(getRequest,RequestOptions.DEFAULT,listener);
    }

    @Override
    public void getSourceRequest(String indexName, String document) {
        GetSourceRequest getSourceRequest = new GetSourceRequest(indexName,document);
        ActionListener<GetSourceResponse> listener =
                new ActionListener<GetSourceResponse>() {
                    @Override
                    public void onResponse(GetSourceResponse getResponse) {

                        Map<String, Object> source = getResponse.getSource();
                        log.info("source:{}",source);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("cause exception:",e.fillInStackTrace());
                    }
                };
        client.getSourceAsync(getSourceRequest, RequestOptions.DEFAULT, listener);
    }
}
