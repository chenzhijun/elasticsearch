package me.chenzhijun.elasticsearch;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@RestController
public class ElasticsearchApplication {

    @Autowired
    private TransportClient client;


    @GetMapping("/")
    public String index() {
        return "index";
    }

    @GetMapping("/get/book/novel")
    public ResponseEntity get(@RequestParam(name = "id", defaultValue = "")
                                      String id) {
        GetResponse result = this.client.
                prepareGet("book", "novel", id).get();

        if (!result.isExists()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity(result.getSource(), HttpStatus.OK);


    }

    @PostMapping("add/book/novel")
    public ResponseEntity add(@RequestParam(name = "title") String title,
                              @RequestParam(name = "author") String author,
                              @RequestParam(name = "wordCount") Integer wordCount,
                              @RequestParam(name = "publishDate") @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date publishDate
    ) {

        XContentBuilder xContent = null;
        IndexResponse result = null;
        try {
            xContent = XContentFactory.jsonBuilder();
            xContent.startObject()
                    .field("title", title)
                    .field("author", author)
                    .field("word_count", wordCount)
                    .field("publish_date", publishDate.getTime())
                    .endObject();
            result = client.prepareIndex("book", "novel")
                    .setSource(xContent)
                    .get();
//            client.prepareIndex().setIndex("book").setType("novel").setId("idtest").setSource(xContent);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity(null, HttpStatus.NOT_FOUND);
        } finally {
            xContent.close();
        }
        return new ResponseEntity(result.getId(), HttpStatus.OK);
    }

    @DeleteMapping("/book/novel")
    public ResponseEntity delete(String id) {
        DeleteResponse response = this.client.prepareDelete("book", "novel", id)
                .get();
        return new ResponseEntity(response.getResult().toString(), HttpStatus.OK);
    }

    @PostMapping("update/book/novel")
    public ResponseEntity update(@RequestParam(name = "id") String id,
                                 @RequestParam(name = "title") String title,
                                 @RequestParam(name = "author") String author,
                                 @RequestParam(name = "wordCount", defaultValue = "445") Integer wordCount
    ) {

        UpdateRequest updateRequest = new UpdateRequest("book", "novel", id);
        XContentBuilder xContentBuilder = null;
        try {
            xContentBuilder = XContentFactory.jsonBuilder();
            xContentBuilder.startObject();
            xContentBuilder.field("title", title);
            xContentBuilder.field("author", title);
            xContentBuilder.field("wordCount", wordCount);
            xContentBuilder.endObject();
            updateRequest.doc(xContentBuilder);
            UpdateResponse response = client.update(updateRequest).get();
            return new ResponseEntity(response.getResult().toString(), HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            xContentBuilder.close();
        }
        return null;
    }

    @PostMapping("query/book/novel")
    public ResponseEntity query(String author,
                                String title,
                                int gtWordCount,
                                int ltWordCount
    ) {

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.matchQuery("author", author));
        boolQuery.must(QueryBuilders.matchQuery("title", title));
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("word_count")
                .from(gtWordCount).to(ltWordCount);

        boolQuery.filter(rangeQuery);
        SearchRequestBuilder builder = client.prepareSearch("book")
                .setTypes("novel")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(boolQuery)
                .setFrom(0)
                .setSize(10);
        System.out.println(builder);
        SearchResponse response = builder.get();
        List<Map<String, Object>> result = new ArrayList<>();
        response.getHits().forEach(hit -> {
            result.add(hit.getSource());
        });
        return new ResponseEntity(result, HttpStatus.OK);
    }

    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchApplication.class, args);
    }
}
