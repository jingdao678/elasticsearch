package com.example.elasticsearch.service.impl;

import com.example.elasticsearch.service.ElasticSearchService;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
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

    @Override
    public void updateDocumentAsync(String indexName, String document) {
     //   UpdateRequest request = new UpdateRequest(indexName,document);
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("updated", new Date());
        jsonMap.put("reason", "daily update"+System.currentTimeMillis());
        jsonMap.put("updateupdate","water=========");
        jsonMap.put("update222","water22222");
        UpdateRequest request = new UpdateRequest(indexName,document)
                .doc(jsonMap);
        request.fetchSource(true);
       ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                String index = updateResponse.getIndex();
                String id = updateResponse.getId();
                long version = updateResponse.getVersion();
                if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                                log.info("update result:"+updateResponse.getResult());
                } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    log.info("update result:"+updateResponse.getResult());

                } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                    log.info("update result:"+updateResponse.getResult());

                } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                    log.info("update result:"+updateResponse.getResult());

                }

                GetResult result = updateResponse.getGetResult();
                if (result.isExists()) {
                    String sourceAsString = result.sourceAsString();
                    Map<String, Object> sourceAsMap = result.sourceAsMap();
                    byte[] sourceAsBytes = result.source();
                    log.info("sourceAsString:{}",sourceAsString);
                } else {

                }

            }

            @Override
            public void onFailure(Exception e) {
                log.error("update fail:",e.fillInStackTrace());
            }
        };
        client.updateAsync(request, RequestOptions.DEFAULT, listener);
    }

    @Override
    public void BulkRequest() {
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("posts").id("1")
                .source(XContentType.JSON,"field", "foo"));
        request.add(new IndexRequest("posts").id("2")
                .source(XContentType.JSON,"field", "bar"));
        request.add(new IndexRequest("posts").id("3")
                .source(XContentType.JSON,"field", "baz"));
        ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {

                for (BulkItemResponse bulkItemResponse : bulkResponse) {

                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure =
                                bulkItemResponse.getFailure();
                    }


                    DocWriteResponse itemResponse = bulkItemResponse.getResponse();

                    switch (bulkItemResponse.getOpType()) {
                        case INDEX:
                        case CREATE:
                            IndexResponse indexResponse = (IndexResponse) itemResponse;
                            break;
                        case UPDATE:
                            UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                            break;
                        case DELETE:
                            DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {

            }
        };

        client.bulkAsync(request, RequestOptions.DEFAULT, listener);
    }
}
