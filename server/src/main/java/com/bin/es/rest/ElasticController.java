package com.bin.es.rest;

import com.bin.es.client.RestFastClient;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: bin.yang
 * @Date: 2019/3/20 16:50
 * @Description:
 */
@Controller
@RequestMapping(value = "/elastic")
@Slf4j
public class ElasticController {

    @Autowired
    private RestFastClient restFastClient;

    /**
     * @Description: (创建索引)
     * @param
     * @[param] []
     * @return void
     * @author:  bin.yang
     * @date:  2019/2/27 4:07 PM
     */
    @RequestMapping(value = "create" , method = RequestMethod.GET)
    @ResponseBody
    public Boolean CreateIndex(){

        RestHighLevelClient client = restFastClient.getClient();

        // 创建 创建索引request 参数：索引名mess
        CreateIndexRequest request = new CreateIndexRequest("data_user_a");

        // 设置索引的settings
        request.settings(Settings.builder()
                // 分片数
                .put("index.number_of_shards", 3)
                // 副本数
                .put("index.number_of_replicas", 1)
        );

        // 设置mapping映射
        Map<String, Object> jsonMap = new HashMap<>();

        Map<String, Object> name = new HashMap<>();
        name.put("type", "text");
        name.put("analyzer", "ik_max_word");

        Map<String, Object> keyName = new HashMap<>();
        keyName.put("type", "keyword");

        Map<String, Object> stdName = new HashMap<>();
        stdName.put("type", "text");
        stdName.put("analyzer", "standard");

        Map<String, Object> suggest = new HashMap<>();
        suggest.put("type", "completion");
        suggest.put("analyzer", "ik_max_word");
        suggest.put("search_analyzer", "ik_max_word");

        Map<String, Object> age = new HashMap<>();
        age.put("type", "integer");

        Map<String, Object> phone = new HashMap<>();
        phone.put("type", "text");
        phone.put("analyzer", "whitespace");

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", name);
        properties.put("key_name", keyName);
        properties.put("std_name", stdName);
        properties.put("suggest", suggest);
        properties.put("age", age);
        properties.put("phone", phone);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);

        jsonMap.put("doc", mapping);
        request.mapping("doc", jsonMap);

        try {
            //  设置索引的别名
            request.alias(new Alias("inx"));

            //  发送请求
            //  同步方式发送请求
            CreateIndexResponse createIndexResponse = client.indices().create(request);

            // 处理响应
            boolean acknowledged = createIndexResponse.isAcknowledged();
            boolean shardsAcknowledged = createIndexResponse.isAcknowledged();
            System.out.println("acknowledged = " + acknowledged);
            System.out.println("shardsAcknowledged = " + shardsAcknowledged);
            return acknowledged;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @Description: (索引文档，即往索引里面放入文档数据.类似于数据库里面向表里面插入一行数据，一行数据就是一个文档)
     * @param
     * @[param] []
     * @return void
     * @author:  bin.yang
     * @date:  2019/2/27 4:07 PM
     */
    @RequestMapping(value = "insert" , method = RequestMethod.GET)
    @ResponseBody
    public Boolean CreateDocument() {

        try (RestHighLevelClient client = restFastClient.getClient()) {
            // 创建索引请求
            IndexRequest request = new IndexRequest(
                    // 索引
                    "data_sou_d_user",
                    // mapping type
                    "t_user",
                    //文档id
                    "3");

            // 以map对象来表示文档
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("name", "不胜人生一场醉");
            jsonMap.put("key_name", "不胜人生一场醉");
            jsonMap.put("std_name", "不胜人生一场醉");
            jsonMap.put("suggest", "不胜人生一场醉");
            jsonMap.put("age", 30);
            jsonMap.put("phone", "10001");
            request.source(jsonMap);

            //、发送请求
            IndexResponse indexResponse = null;
            try {
                // 同步方式
                indexResponse = client.index(request);
            } catch (ElasticsearchException e) {
                // 捕获，并处理异常
                //判断是否版本冲突、create但文档已存在冲突
                if (e.status() == RestStatus.CONFLICT) {
                    log.error("冲突了，请在此写冲突处理逻辑！\n" + e.getDetailedMessage());
                }

                log.error("索引异常", e);
            }

            //、处理响应
            if (indexResponse != null) {
                String index = indexResponse.getIndex();
                String type = indexResponse.getType();
                String id = indexResponse.getId();
                long version = indexResponse.getVersion();
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("新增文档成功，处理逻辑代码写到这里。");
                } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    System.out.println("修改文档成功，处理逻辑代码写到这里。");
                }
                // 分片处理信息
                ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

                }
                // 如果有分片副本失败，可以获得失败原因信息
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        String reason = failure.reason();
                        System.out.println("副本失败原因：" + reason);
                    }
                }
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @Description: (获取文档数据)
     * @param
     * @[param] []
     * @return void
     * @author:  bin.yang
     * @date:  2019/2/27 4:08 PM
     */
    @RequestMapping(value = "find" , method = RequestMethod.GET)
    @ResponseBody
    public void GetDocument(String did) {

        try (RestHighLevelClient client = restFastClient.getClient()) {
            // 创建获取文档请求
            GetRequest request = new GetRequest(
                    //索引
                    "data_sou",
                    // mapping type
                    "t_user",
                    //文档id
                    did);

            //选择返回的字段
            String[] includes = new String[]{"user","message", "*Date"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext);

            //、发送请求
            GetResponse getResponse = null;
            try {
                // 同步请求
                getResponse = client.get(request);
            } catch (ElasticsearchException e) {
                if (e.status() == RestStatus.NOT_FOUND) {
                    log.error("没有找到该id的文档" );
                }
                if (e.status() == RestStatus.CONFLICT) {
                    log.error("获取时版本冲突了，请在此写冲突处理逻辑！" );
                }
                log.error("获取文档异常", e);
            }

            //、处理响应
            if(getResponse != null) {
                String index = getResponse.getIndex();
                String type = getResponse.getType();
                String id = getResponse.getId();
                // 文档是否存在
                if (getResponse.isExists()) {
                    long version = getResponse.getVersion();
                    //结果取成 String
                    String sourceAsString = getResponse.getSourceAsString();
                    System.err.println(sourceAsString);
                    // 结果取成Map
                    Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                    //结果取成字节数组
                    byte[] sourceAsBytes = getResponse.getSourceAsBytes();

                    log.info("index:" + index + "  type:" + type + "  id:" + id);
                    log.info(sourceAsString);

                } else {
                    log.error("没有找到该id的文档" );
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * @Description: (删除文档)
     * @param
     * @[param] []
     * @return void
     * @author:  bin.yang
     * @date:  2019/2/27 4:37 PM
     */
    @RequestMapping(value = "delete" , method = RequestMethod.GET)
    @ResponseBody
    public void DelDocument(String zid) {

        try (RestHighLevelClient client = restFastClient.getClient()) {
            // 、创建获取文档请求
            DeleteRequest request = new DeleteRequest(
                    // 索引
                    "data_user_a",
                    // mapping type
                    "doc",
                    //文档id
                    zid);

            //、发送请求
            DeleteResponse deleteResponse = null;
            try {
                // 同步请求
                deleteResponse = client.delete(request);
            } catch (ElasticsearchException e) {
                if (e.status() == RestStatus.NOT_FOUND) {
                    log.error("没有找到该id的文档" );
                }
                if (e.status() == RestStatus.CONFLICT) {
                    log.error("获取时版本冲突了，请在此写冲突处理逻辑！" );
                }
                log.error("获取文档异常", e);
            }

            //、处理响应
            if(deleteResponse != null) {
                String index = deleteResponse.getIndex();
                String type = deleteResponse.getType();
                String id = deleteResponse.getId();
                log.error("文档"+ id+" : 删除成功");
                long version = deleteResponse.getVersion();
                System.err.println(deleteResponse.toString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
