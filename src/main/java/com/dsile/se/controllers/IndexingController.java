package com.dsile.se.controllers;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@EnableAutoConfiguration
public class IndexingController {

    @RequestMapping(path = "/utils", method = RequestMethod.GET)
    String buildIndex() {
        return "{\"index\": \"index\"}";
    }
}
