package com.lastingwar.utils.es;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;


/**
 * @author yhm
 * @create 2020-11-09 16:55
 */
public class EsWrite {
    public static void main(String[] args) throws IOException {

    // 1.创建ES客户端构建器
    JestClientFactory factory = new JestClientFactory();

    // 2.创建ES客户端连接地址
    HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

    // 3.设置ES连接地址
    factory.setHttpClientConfig(httpClientConfig);

    // 4.获取ES客户端连接
    JestClient jestClient = factory.getObject();

    // 5.构建ES插入数据对象
    // 可以使用javabean
    Index index = new Index.Builder(" {\n" +
            "  \"id\":\"1003\",\n" +
            "  \"name\":\"最后的进化\",\n" +
            "  \"doubanScore\":\"6.5\"\n" +
            "}")
            .index("movie_2020")
            .type("movie")
            .id("1003")
            .build();

    jestClient.execute(index);

    jestClient.shutdownClient();

    }
}
