package com.dsile.se.utils;

import com.dsile.se.SearchExpressionEvaluator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;


public class IndexSearcher {

    private HashMap<String, Integer> termDictionary = new HashMap<>();
    private HashMap<Integer,Long> termIndexLinks = new HashMap<>();
    private HashMap<Integer,String> docsTitleMap = new HashMap<>();

    public Set<Integer> findDocsWithWord(String word) {

        Set<Integer> resultDocs = new HashSet<>();

        try {
            try(RandomAccessFile memoryMappedFile = new RandomAccessFile(Constants.RESULT_INDEX_PATH, "r")){
                System.out.println("index: " + termDictionary.get(word));
                if(termDictionary.get(word) == null){
                    return resultDocs;
                }
                long place = termIndexLinks.get(termDictionary.get(word));
                MappedByteBuffer in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, place, Integer.BYTES);
                long bufferSize = in.getInt();
                System.out.println(bufferSize/Integer.BYTES);
                in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, place + Integer.BYTES , bufferSize);
                for(int i = 0; i < bufferSize; i += Integer.BYTES){
                    resultDocs.add(in.getInt());
                }
            }
        } catch (IOException e) {
            System.out.println(e);//TODO: Logging
            return resultDocs;
        }

        return resultDocs;
    }

    public void lazyLoading() throws IOException, ClassNotFoundException {
        if(termDictionary.isEmpty() || termIndexLinks.isEmpty() || docsTitleMap.isEmpty()){
            try(ObjectInputStream termReader = new ObjectInputStream(new FileInputStream(Constants.TERM_DICTIONARY_PATH));
                ObjectInputStream titleReader = new ObjectInputStream(new FileInputStream(Constants.DOCS_TITLES_PATH));
                ObjectInputStream linkReader = new ObjectInputStream(new FileInputStream(Constants.INDEX_LINKS_PATH))){
                System.out.println("start loading");
                termDictionary = (HashMap<String,Integer>)termReader.readObject();
                System.out.println(termDictionary.size());
                System.out.println("terms loaded");
                termIndexLinks = (HashMap<Integer,Long>)linkReader.readObject();
                System.out.println("links loaded");
                docsTitleMap = (HashMap<Integer,String>)titleReader.readObject();
                System.out.println("titles loaded");
            }
        }
    }

    public String getDocTitleById(int id){
        return docsTitleMap.get(id);
    }

    public Set<Integer> allDocs(){
        return docsTitleMap.keySet();
    }

    //TODO: Make test and remove main
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        IndexSearcher is = new IndexSearcher();
        SearchExpressionEvaluator see = new SearchExpressionEvaluator(is);
        is.lazyLoading();
    }

}
