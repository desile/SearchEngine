package com.dsile.se.dto;

/**
 * Created by desile on 04.03.17.
 */
public class DocumentDto {

    private int id;
    private String title;

    public DocumentDto(int id, String title){
        this.id = id;
        this.title = title;
    }

    public int getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }
}
