package com.shouzan.es.rest;

import com.shouzan.es.client.RestFastClient;
import com.shouzan.es.entity.SearchEngine;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author: bin.yang
 * @Date: 2019/3/20 16:51
 * @Description:
 */
@Controller
@RequestMapping(value = "/elasticSenior")
@Slf4j
public class ElasticSeniorController {

    @Autowired
    private RestFastClient restFastClient;

    /**
     * @Description: (关键字分页检索)
     * @param searchEngine
     * @[param] [searchEngine]
     * @return java.util.List<java.lang.Object>
     * @author:  bin.yang
     * @date:  2019/4/10 4:12 PM
     */
    @RequestMapping(value ="/page" , method = RequestMethod.GET)
    @ResponseBody
    public List<Object> searchDocument(SearchEngine searchEngine){

        try (RestHighLevelClient client = restFastClient.getClient()) {

            // 创建search请求
            SearchRequest searchRequest = new SearchRequest("data_user_a");

            // 用SearchSourceBuilder来构造查询请求体 ,请仔细查看它的方法，构造各种查询的方法都在这。
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            // 关键字搜索
            if(searchEngine.getKeyword() != null && !"".equals(searchEngine.getKeyword())){

                // 设置boolQueryBuilder条件
                BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

                // 模糊查询字段匹配
                WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery("keyname", "*" + searchEngine.keyword() + "*");

                // 构造容错匹配
                QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", searchEngine.keyword())
                        .fuzziness(Fuzziness.AUTO)
                        .prefixLength(3)
                        .maxExpansions(30)
                        .analyzer("ik_max_word");

                // 前缀间隔匹配
                MatchPhrasePrefixQueryBuilder prefixQueryBuilder = QueryBuilders.matchPhrasePrefixQuery("stdname", searchEngine.keyword())
                        .slop(3);

                // 子boolQueryBuilder条件条件，用来表示查询条件or的关系
                boolQueryBuilder.should(matchQueryBuilder)
                        .should(wildcardQuery)
                        .should(prefixQueryBuilder)
                ;

                // 添加查询条件到boolQueryBuilder中
                sourceBuilder.query(boolQueryBuilder);

                // 高亮设置
                HighlightBuilder highlighter = new HighlightBuilder()
                        .field("name")
                        .preTags("<strong><font color='#CC0000'>")
                        .postTags("</font></strong>");
                sourceBuilder.highlighter(highlighter);

            }

            // 查询位置-分页
            sourceBuilder.from(searchEngine.start());
            sourceBuilder.size(searchEngine.end());
            sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

            //指定排序
            sourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.DESC));

            //将请求体加入到请求中
            searchRequest.source(sourceBuilder);

            //3、发送请求
            SearchResponse searchResponse = client.search(searchRequest);

            System.err.println("检索结束");
            //处理搜索命中文档结果
            SearchHits hits = searchResponse.getHits();

            //总条数
            long totalHits = hits.getTotalHits();
            float maxScore = hits.getMaxScore();

            System.err.println(totalHits + "_______" + maxScore);

            List<Object> objects = new ArrayList<>();

            // 检索到的list数组
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {

                String index = hit.getIndex();
                String type = hit.getType();
                String id = hit.getId();
                float score = hit.getScore();

                // 获取高亮信息
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highField = highlightFields.get("name");

                String sourceAsString = hit.getSourceAsString();

                //取成map对象
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();

                // 试验代码 没啥用
                sourceAsMap.remove("keyname");
                sourceAsMap.remove("stdname");
                sourceAsMap.remove("suggest");

                // 加入高亮数据
                if(highField != null){
                    Text[] fragments = highField.getFragments();
                    sourceAsMap.put("High",fragments[0].toString());
                }

                System.err.println("index:" + index + "  type:" + type + "  id:" + id);
                sourceAsMap.put("id",id);
                objects.add(sourceAsMap);
            }
            System.err.println(objects);
            return objects;
        } catch (IOException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    /**
     * @Description: (查询建议)
     * @param keyword
     * @[param] [keyword]
     * @return void
     * @author:  bin.yang
     * @date:  2019/4/12 4:21 PM
     */
    @RequestMapping(value = "/suggest" , method = RequestMethod.GET)
    @ResponseBody
    public List<String> SuggestDocument(String keyword){

        try (RestHighLevelClient client = restFastClient.getClient()) {

            // 创建search请求
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("data_sou_d_user");

            // 用SearchSourceBuilder来构造查询请求体 ,请仔细查看它的方法，构造各种查询的方法都在这。
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //做查询建议
            //词项建议
            SuggestionBuilder termSuggestionBuilder =SuggestBuilders.termSuggestion("keyname")
                    .text(keyword)
                    .size(5).maxEdits(2)
                    .suggestMode(TermSuggestionBuilder.SuggestMode.ALWAYS);

            SuggestBuilder suggestBuilder = new SuggestBuilder();

            suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);

            sourceBuilder.suggest(suggestBuilder);

            searchRequest.source(sourceBuilder);

            //发送请求
            SearchResponse searchResponse = client.search(searchRequest);

            List<String> strings = new ArrayList<>();

            //处理响应
            //搜索结果状态信息
            if(RestStatus.OK.equals(searchResponse.status())) {
                // 获取建议结果
                Suggest suggest = searchResponse.getSuggest();
                TermSuggestion termSuggestion = suggest.getSuggestion("suggest_user");
                for (TermSuggestion.Entry entry : termSuggestion.getEntries()) {
                    log.info("text: " + entry.getText().string());
                    for (TermSuggestion.Entry.Option option : entry) {
                        String suggestText = option.getText().string();
                        strings.add(" 你要找的是不是 : " + suggestText);
                    }
                }
            }
            return strings;
        } catch (IOException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    /**
     * @Description: (自动补全)
     * @param keyword
     * @[param] [keyword]
     * @return java.util.List<java.lang.String>
     * @author:  bin.yang
     * @date:  2019/4/12 5:40 PM
     */
    @RequestMapping(value = "/comple" , method = RequestMethod.GET)
    @ResponseBody
    public List<String> completionSuggester(String keyword) {

        try (RestHighLevelClient client = restFastClient.getClient()) {

            // 创建search请求
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("data_sou_d_user");

            // 用SearchSourceBuilder来构造查询请求体 ,请仔细查看它的方法，构造各种查询的方法都在这。
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            SuggestionBuilder termSuggestionBuilder = SuggestBuilders.completionSuggestion("suggest")
                            .prefix(keyword)
                            .skipDuplicates(true);
            SuggestBuilder suggestBuilder = new SuggestBuilder();

            suggestBuilder.addSuggestion("song-suggest", termSuggestionBuilder);

            sourceBuilder.suggest(suggestBuilder);

            searchRequest.source(sourceBuilder);

            //发送请求
            SearchResponse searchResponse = client.search(searchRequest);

            List<String> strings = new ArrayList<>();

            //处理响应
            //搜索结果状态信息
            if(RestStatus.OK.equals(searchResponse.status())) {
                // 获取建议结果
                Suggest suggest = searchResponse.getSuggest();
                CompletionSuggestion termSuggestion = suggest.getSuggestion("song-suggest");
                for (CompletionSuggestion.Entry entry : termSuggestion.getEntries()) {
                    log.info("text: " + entry.getText().string());
                    for (CompletionSuggestion.Entry.Option option : entry) {
                        String suggestText = option.getText().string();
                        strings.add(" 你要找的是不是 : " + suggestText);
                    }
                }
            }
            return strings;
        } catch (IOException e) {
            log.error(e.getMessage());
            return null;
        }
    }

}
