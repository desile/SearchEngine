package com.dsile.se.dto;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class IndexTermRecord implements Comparable<IndexTermRecord>{

    private int docId;
    private List<Integer> positions;
    private int positionsSize;
    private float tfIdf = 0;

    public IndexTermRecord(List<Integer> docIdAndPositions) {
        this.docId = docIdAndPositions.get(0);
        this.positions = docIdAndPositions.subList(1,docIdAndPositions.size());
        Collections.reverse(this.positions);
        this.positionsSize = this.positions.size();
    }

    public IndexTermRecord(int docId, float tfIdf, List<Integer> positions){
        this.docId = docId;
        this.tfIdf = tfIdf;
        this.positions = new LinkedList<>(positions);
        this.positionsSize = this.positions.size();
    }

    public void sumTfIdf(float addingTfIdf) {
        this.tfIdf += addingTfIdf;
    }

    public float getTfIdf() {
        return tfIdf;
    }

    public int getDocId() {
        return docId;
    }

    public List<Integer> getPositions() {
        return positions;
    }

    public float calculateIf(){
        return 1 + (float)Math.log(positions.size());
    }

    public int getPositionsSize() {
        return positionsSize;
    }

    public void resetTfIdf(){
        this.tfIdf = 0;
    }


    @Override
    public int compareTo(IndexTermRecord indexTermRecord) {
        return indexTermRecord.getDocId() - this.docId;
    }
}