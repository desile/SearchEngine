package com.dsile.se.dto;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by desile on 26.03.17.
 */
public class IndexTermRecord {

    private int termId;
    private List<IndexDocumentRecord> documents;

    public IndexTermRecord (int termId){
        this.termId = termId;
        this.documents = new LinkedList<>();
    }

    public void addDocuments(IndexDocumentRecord document){
        this.documents.add(document);
    }

    public int getTermId() {
        return termId;
    }

    public List<IndexDocumentRecord> getDocuments() {
        return documents;
    }
}
