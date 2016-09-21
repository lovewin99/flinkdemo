package com.wangxy.stream;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by wangxy on 16-9-21.
 */

public class TestElasticsearchSinkFunction implements ElasticsearchSinkFunction<String>  {
//    private static final long serialVersionUID = 1L;

    public IndexRequest createIndexRequest(String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("flink").id("hash"+element).source(json);
    }

    @Override
    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//        indexer.add(createIndexRequest(element));
        System.out.println("element = " + element);
    }
}
