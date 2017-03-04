package com.dsile.se.controllers;

import com.dsile.se.SearchExpressionEvaluator;
import com.dsile.se.dto.DocumentDto;
import com.dsile.se.utils.IndexSearcher;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.FileWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Controller
public class MainPageController {

    private IndexSearcher is = new IndexSearcher();
    private SearchExpressionEvaluator see = new SearchExpressionEvaluator(is);

    @RequestMapping("/")
    public String mainPage(Model model, @RequestParam(name="query", required = false) String query){
        model.addAttribute("query", query);
        if(query == null || query.isEmpty()){
            model.addAttribute("res", Collections.emptySet());
            return "index";
        }
        String formatedQuery = query.replace(" ","&&");
        try {
            is.lazyLoading();
        } catch (Exception e) {
            System.out.println(e);
            model.addAttribute("res", Collections.emptySet());
            return "index";
        }
        System.out.println(String.format("Query: %s", query));
        List resut = see.evaluate(formatedQuery).stream().map(docId -> new DocumentDto(docId,is.getDocTitleById(docId))).collect(Collectors.toList());
        if(resut.size() > 1000){
            resut = resut.subList(0,1000);
        }
        model.addAttribute("res", resut);
        return "index";
    }

}
