package com.dsile.se.controllers;

import com.dsile.se.SearchExpressionEvaluator;
import com.dsile.se.dto.DocumentDto;
import com.dsile.se.dto.QueryResultDto;
import com.dsile.se.utils.IndexSearcher;
import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@EnableAutoConfiguration
public class SearchController {

    @Autowired
    private IndexSearcher is;
    @Autowired
    private SearchExpressionEvaluator see;

    @RequestMapping(path = "/search", method = RequestMethod.GET, produces = "application/json; charset=utf-8")
    String search(@RequestParam(name="query") String query ,
                  @RequestParam(name="type") String searchType ,
                  @RequestParam(name="page", defaultValue = "0") int page) {
        int contentLength = 100;

        if(query == null || query.isEmpty()){
            return "{\"err\": \"empty query\"}";
        }

        try {
            is.lazyLoading();
        } catch (Exception e){
            System.out.println("Не удалось выполнить загрузку словарей");
        }

        long st = System.nanoTime();

        List<DocumentDto> findingDocs = see.parseAndEvaluate(query, searchType).entrySet().stream().map(docId -> new DocumentDto(docId.getKey(), is.getDocTitleById(docId.getKey()), docId.getValue().getTfIdf())).collect(Collectors.toList());
        int resultSize = findingDocs.size();

        switch (searchType) {
            case "tf_idf":
                findingDocs.sort(DocumentDto::compareTo);
                break;
            case "boolean":
                break;
        }

        long en = System.nanoTime();

        long queryTime = en - st;

        System.out.println("Complete in time: " + String.format("%,12d",(queryTime)));

        if (findingDocs.size() > contentLength) {
            findingDocs = findingDocs.subList(page*contentLength, (page+1)*contentLength);
        }

        QueryResultDto result = new QueryResultDto(findingDocs,resultSize,page);
        result.setTimeQueryProcessing(queryTime);

        return new Gson().toJson(result);
    }
}
