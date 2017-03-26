package com.dsile.se.utils;

import com.dsile.se.dto.IndexDocumentRecord;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;


@Component
public class IndexSearcher {

    private HashMap<String, Integer> termDictionary = new HashMap<>();
    private HashMap<Integer, Long> termIndexLinks = new HashMap<>();
    private HashMap<Integer, String> docsTitleMap = new HashMap<>();


    private boolean quoteRecursiveFinder(int wordNumber, int position, int connectivitiy, int wordCount, List<SortedMap<Integer, IndexDocumentRecord>> quoteDocs, int docId){
        List<Integer> positions = quoteDocs.get(wordNumber).get(docId).getPositions();

        for (int i = 0; i < positions.size(); i++) {
            if(positions.get(i) < position){
                continue;//следующая позиция должна быть больше предыдущей
            } else {
                if(positions.get(i) <= (position + connectivitiy)){ //позиция должна быть больше, но при этом вписываться в окно
                    if(wordNumber == wordCount - 1){ //если это последнее слово в цитате то выходим
                        return true;
                    } // если не последнее то ныряем дальше в рекурсию
                    return quoteRecursiveFinder(wordNumber + 1,positions.get(i),connectivitiy - (positions.get(i) - position),wordCount,quoteDocs,docId);
                } else { // если она не вписалась в окно, то дальше позиции будут только больше можно выходить из рекурсии
                    return false;
                }
            }
        }
        return false;
    }

    private void collectRecordsFromIndexBlockByWord(String word, SortedMap<Integer, IndexDocumentRecord> newWord) throws IOException {
        try (RandomAccessFile memoryMappedFile = new RandomAccessFile(Constants.RESULT_INDEX_PATH, "r")) {

            System.out.println("index: " + termDictionary.get(word));
            if (termDictionary.get(word) == null) {
                return;
            }

            long place = termIndexLinks.get(termDictionary.get(word));
            MappedByteBuffer in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, place, Integer.BYTES);
            long bufferSize = in.getInt();
            System.out.println(bufferSize / Integer.BYTES);
            in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, place + Integer.BYTES, bufferSize);
            List<Integer> positions = new LinkedList<>();
            while (in.hasRemaining()) {
                int docId = in.getInt();
                float tfIdf = in.getFloat();
                int posSize = in.getInt();
                for (int j = 0; j < posSize; j++) {
                    positions.add(in.getInt());
                }

                IndexDocumentRecord curRecord = newWord.get(docId);
                if (curRecord != null) {
                    curRecord.sumTfIdf(tfIdf);
                } else {
                    newWord.put(docId, new IndexDocumentRecord(docId, tfIdf, positions));
                }
                positions.clear();
            }
        }
    }

    private SortedMap<Integer, IndexDocumentRecord> collectRecordsFromIndexBlockByWord(String word) throws IOException {
        SortedMap<Integer, IndexDocumentRecord> newWord = new TreeMap<>();
        collectRecordsFromIndexBlockByWord(word, newWord);
        return newWord;
    }


    public SortedMap<Integer,IndexDocumentRecord> findDocsWithQuote(List<String> words, int connectivity) {
        List<SortedMap<Integer, IndexDocumentRecord>> quoteDocs = new LinkedList<>();

        try {
            for (String word : words) {
                quoteDocs.add(collectRecordsFromIndexBlockByWord(word));
            }
        } catch (IOException e) {
            System.out.println(e);//TODO: Logging
        }

        //find intersections of all words in quotes
        Set<Integer> commonDocs = new TreeSet<>(quoteDocs.get(0).keySet());
        if(quoteDocs.size() < 2){
            return quoteDocs.get(0);
        } else {
            for(int i = 1; i < quoteDocs.size(); i++){
                commonDocs.retainAll(quoteDocs.get(i).keySet());
            }

            for(int i = 0; i < quoteDocs.size(); i++){
                quoteDocs.get(i).keySet().retainAll(commonDocs);
            }
        }

        //check words with connectivity
        docCycle:
        for(int docId : commonDocs){
            for(int i = 0; i < quoteDocs.get(0).get(docId).getPositions().size(); i++) {
                if (quoteRecursiveFinder(1, quoteDocs.get(0).get(docId).getPositions().get(i), connectivity, quoteDocs.size(), quoteDocs, docId)) {
                    quoteDocs.get(0).get(docId).resetTfIdf();
                    continue docCycle;
                }
            }
            quoteDocs.get(0).remove(docId);
        }

        return quoteDocs.get(0);
    }

    public SortedMap<Integer,IndexDocumentRecord> findDocsWithWord(List<String> normalForms) {

        SortedMap<Integer, IndexDocumentRecord> resultDocs = new TreeMap<>();

        try {
            for (String word : normalForms) {
                collectRecordsFromIndexBlockByWord(word,resultDocs);
            }
        } catch (IOException e) {
            System.out.println(e);//TODO: Logging
            return resultDocs;
        }

        return resultDocs;
    }

    public void lazyLoading() throws IOException, ClassNotFoundException {
        if (termDictionary.isEmpty() || termIndexLinks.isEmpty() || docsTitleMap.isEmpty()) {
            try (ObjectInputStream termReader = new ObjectInputStream(new FileInputStream(Constants.TERM_DICTIONARY_PATH));
                 ObjectInputStream titleReader = new ObjectInputStream(new FileInputStream(Constants.DOCS_TITLES_PATH));
                 ObjectInputStream linkReader = new ObjectInputStream(new FileInputStream(Constants.INDEX_LINKS_PATH))) {
                System.out.println("start loading");
                termDictionary = (HashMap<String, Integer>) termReader.readObject();
                System.out.println(termDictionary.size());
                System.out.println("terms loaded");
                termIndexLinks = (HashMap<Integer, Long>) linkReader.readObject();
                System.out.println("links loaded");
                docsTitleMap = (HashMap<Integer, String>) titleReader.readObject();
                System.out.println("titles loaded");
            }
        }
    }

    public String getDocTitleById(int id) {
        return docsTitleMap.get(id);
    }

    public Set<Integer> allDocs() {
        return docsTitleMap.keySet();
    }


}
