package com.lastingwar.utils.es;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.MinAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author yhm
 * @create 2020-11-09 18:07
 */
public class EsRead {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端构建器
        JestClientFactory factory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置ES连接地址
        factory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = factory.getObject();

        // 创建searchSource
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 添加查询条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder class_id = new TermQueryBuilder("class_id", "0621");
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo2", "球");
        boolQueryBuilder.filter(class_id);
        boolQueryBuilder.must(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        // 聚合条件
        MinAggregationBuilder minAgeGroup = AggregationBuilders.min("minAge").field("age");

        TermsAggregationBuilder genderCount = AggregationBuilders.terms("countByGender").field("gender");

        searchSourceBuilder.aggregation(minAgeGroup);
        searchSourceBuilder.aggregation(genderCount);

        // 分页
        searchSourceBuilder.size(10);
        searchSourceBuilder.from(0);
        // 生成查询指令
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student2")
                .addType("_doc")
                .build();

        //6.执行插入数据操作
        SearchResult result = jestClient.execute(search);

        Long total = result.getTotal();

        System.out.println(total);


        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            System.out.println("**************");
            for (Object o : source.keySet()) {
                System.out.println("Key:" + o + ",Value:" + source.get(o));
            }
        }

        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation countByGender = aggregations.getTermsAggregation("countByGender");
        for (TermsAggregation.Entry entry : countByGender.getBuckets()) {
            System.out.println(entry.getKeyAsString() + ":" + entry.getCount());
        }

        MinAggregation minAge = aggregations.getMinAggregation("minAge");
        System.out.println(minAge.getMin());
        //7.关闭连接
        jestClient.shutdownClient();


    }
}
