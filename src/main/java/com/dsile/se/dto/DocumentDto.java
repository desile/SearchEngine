package com.dsile.se.dto;

/**
 * Created by desile on 04.03.17.
 */
public class DocumentDto implements Comparable<DocumentDto> {

    private int id;
    private String title;
    private float tfIdf;

    public DocumentDto(int id, String title, float tfIdf){
        this.id = id;
        this.title = title;
        this.tfIdf = tfIdf;
    }

    public int getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public float getTfIdf() {
        return tfIdf;
    }

    @Override
    public int compareTo(DocumentDto doc) {
        if(this.getTfIdf() < doc.getTfIdf()){
            return 1;
        } else if (this.getTfIdf() > doc.getTfIdf()) {
            return -1;
        } else {
            return 0;
        }
    }
}
