package com.bin.es.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @Author: bin.yang
 * @Date: 2019/3/22 10:19
 * @
 * @Description:
 */
@Component
public class RestFastClient {

    @Value("${elasticsearch.server.url}")
    private String host;

    @Value("${elasticsearch.server.port}")
    private Integer port;

    @Value("${elasticsearch.server.quest}")
    private String quest;

    private RestHighLevelClient client;

    /**
     * @Description: (创建连接)
     * @param
     * @[param] []
     * @return org.elasticsearch.client.RestHighLevelClient
     * @author:  bin.yang
     * @date:  2019/3/22 3:25 PM
     */
    public  RestHighLevelClient getClient() {

        //rest高级客户端实例需要一个REST低级别的客户端生成器 来构建
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, quest),
                        new HttpHost(host, port, quest)));

        return client;
    }

    /**
     * @Description: (关闭连接)
     * @param
     * @[param] []
     * @return void
     * @author:  bin.yang
     * @date:  2019/3/22 3:25 PM
     */
    public void closeClient() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
