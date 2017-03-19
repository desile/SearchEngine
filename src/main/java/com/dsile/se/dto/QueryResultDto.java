package com.dsile.se.dto;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by desile on 19.03.17.
 */
public class QueryResultDto {

    private List<DocumentDto> documents;
    private int resultSize;
    private int page;

    public QueryResultDto(List<DocumentDto> documents, int resultSize, int page) {
        this.documents = documents;
        this.resultSize = resultSize;
        this.page = page;
    }

    public List<DocumentDto> getDocuments() {
        return documents;
    }

    public void setDocuments(List<DocumentDto> documents) {
        this.documents = documents;
    }

    public int getResultSize() {
        return resultSize;
    }

    public void setResultSize(int resultSize) {
        this.resultSize = resultSize;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }
}
