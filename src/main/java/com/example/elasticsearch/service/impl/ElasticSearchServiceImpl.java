package com.example.elasticsearch.service.impl;

import com.example.elasticsearch.service.ElasticSearchService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Date;

@Service
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
}
