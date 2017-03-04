package com.dsile.se.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class MainPageController {

    @RequestMapping("/")
    public String mainPage(Model model){
        model.addAttribute("name", "Pavel");
        return "index";
    }

}
