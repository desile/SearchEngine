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
    int minimalGapListSize = 50;


    private boolean quoteRecursiveFinder(int wordNumber, int position, int connectivitiy, int wordCount, List<Map<Integer, IndexDocumentRecord>> quoteDocs, int docId){
        List<Integer> positions = quoteDocs.get(wordNumber).get(docId).getPositions();
        int gapSize = (int)(Math.sqrt(positions.size()));

        for (int i = 0; i < positions.size(); i++) {
            if(positions.get(i) < position){
                if(i + gapSize < positions.size() && positions.get(i + gapSize) < position){
                    i += gapSize;
                    continue;
                } else {
                    continue;//следующая позиция должна быть больше предыдущей
                }
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

    private List<Map<Integer, IndexDocumentRecord>> collectRecordsWithFastIntersectiont(List<String> words) throws IOException {
        List<Map<Integer, IndexDocumentRecord>> quoteDocs = new LinkedList<>();
        for(String word : words){
            quoteDocs.add(new HashMap<>());
        }

        List<MappedByteBuffer> buffers = new LinkedList<>();
        List<Integer> docsSize = new LinkedList<>();
        List<Integer> gapsSize = new LinkedList<>();
        List<Integer> currentDoc = new LinkedList<>();
        List<Integer> iter = new LinkedList<>();

        for(String word : words){
            RandomAccessFile memoryMappedFile = new RandomAccessFile(Constants.RESULT_INDEX_PATH, "r");
            long place = termIndexLinks.get(termDictionary.get(word));
            MappedByteBuffer in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, place, Integer.BYTES);
            long bufferSize = in.getInt();
            in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, place + Integer.BYTES, bufferSize);
            buffers.add(in);
            iter.add(0);
            docsSize.add(in.getInt());
            currentDoc.add(in.getInt());
        }

        for(int i = 0; i < docsSize.size(); i++){
            gapsSize.add((int)(Math.sqrt(docsSize.get(i))));
        }

        try {

            intersectionCycle:
            while (true) {

                int minId = currentDoc.stream().min(Integer::compareTo).get();
                int maxId = currentDoc.stream().max(Integer::compareTo).get();

                List<Integer> indexesWithMinId = new LinkedList<>();
                for (int i = 0; i < currentDoc.size(); i++) {
                    if (currentDoc.get(i) == minId) {
                        indexesWithMinId.add(i);
                    }
                }

                for (int i : indexesWithMinId) {
                    if (minimalGapListSize <= docsSize.get(i) && iter.get(i) % gapsSize.get(i) == 0 && iter.get(i) + gapsSize.get(i) < docsSize.get(i)) {
                        int gapBytes = buffers.get(i).getInt();
                        int docIdOnGap = buffers.get(i).getInt(buffers.get(i).position() + gapBytes);
                        if (docIdOnGap < maxId) {
                            currentDoc.set(i, docIdOnGap);
                            buffers.get(i).position(buffers.get(i).position() + gapBytes + Integer.BYTES);
                            iter.set(i, iter.get(i) + gapsSize.get(i));// -1 ?
                            continue intersectionCycle;
                        }
                    }
                }

                if (indexesWithMinId.size() == words.size()) {
                    for (int i = 0; i < buffers.size(); i++) {
                        float tfIdf = buffers.get(i).getFloat();
                        int posSize = buffers.get(i).getInt();
                        List<Integer> positions = new ArrayList<>(posSize);
                        for (int j = 0; j < posSize; j++) {
                            positions.add(buffers.get(i).getInt());
                        }
                        IndexDocumentRecord idr = new IndexDocumentRecord(currentDoc.get(i), tfIdf, positions);
                        quoteDocs.get(i).put(currentDoc.get(i), idr);
                        if (!buffers.get(i).hasRemaining()) {
                            break intersectionCycle;
                        }

                        iter.set(i, iter.get(i) + 1);
                        currentDoc.set(i, buffers.get(i).getInt());

                    }
                } else {
                    for (int minIndex : indexesWithMinId) {
                        buffers.get(minIndex).getFloat();
                        int posSize = buffers.get(minIndex).getInt();
                        buffers.get(minIndex).position(buffers.get(minIndex).position() + posSize * Integer.BYTES);

                        if (!buffers.get(minIndex).hasRemaining()) {
                            break intersectionCycle;
                        }
                        iter.set(minIndex, iter.get(minIndex) + 1);

                        currentDoc.set(minIndex, buffers.get(minIndex).getInt());
                    }
                }

            }

        } catch (IndexOutOfBoundsException iooe){
            iooe.printStackTrace();
        }
        return quoteDocs;
    }

    private void collectRecordsFromIndexBlockByWord(String word, Map<Integer, IndexDocumentRecord> newWord) throws IOException {
        try (RandomAccessFile memoryMappedFile = new RandomAccessFile(Constants.RESULT_INDEX_PATH, "r")) {

            System.out.println("index: " + termDictionary.get(word));
            if (termDictionary.get(word) == null) {
                return;
            }

            long place = termIndexLinks.get(termDictionary.get(word));
            MappedByteBuffer in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, place, Integer.BYTES);
            long bufferSize = in.getInt();
            in = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, place + Integer.BYTES, bufferSize);
            List<Integer> positions = Collections.emptyList();
            int docsSetSize = in.getInt();
            int gapsSize = (int)(Math.sqrt(docsSetSize));
            int iter = 0;
            while (in.hasRemaining()) {
                int docId = in.getInt();
                //такое условие из-за того, что прыжки установлены в строго определенных местах
                if(minimalGapListSize <= docsSetSize && iter % gapsSize == 0 && iter + gapsSize < docsSetSize){
                    in.getInt();//прыжки тут не нужны, поэтому пропускаем
                }
                float tfIdf = in.getFloat();
                int posSize = in.getInt();
                in.position(in.position() + posSize * Integer.BYTES);

                IndexDocumentRecord curRecord = newWord.get(docId);
                if (curRecord != null) {
                    curRecord.sumTfIdf(tfIdf);
                } else {
                    newWord.put(docId, new IndexDocumentRecord(docId, tfIdf, positions));
                }

                iter++;
            }

            System.out.println("index: " + termDictionary.get(word) + " complete");
        }
    }


    public Map<Integer,IndexDocumentRecord> findDocsWithQuote(List<String> words, int connectivity) {
        List<Map<Integer, IndexDocumentRecord>> quoteDocs = new LinkedList<>();

        try {
            quoteDocs = collectRecordsWithFastIntersectiont(words);
        } catch (IOException e) {
            System.out.println(e);//TODO: Logging
        }

        if(quoteDocs.size() < 2){
            return quoteDocs.get(0);
        }

        //check words with connectivity
        docCycle:
        for(int docId : quoteDocs.get(1).keySet()){
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

    public Map<Integer,IndexDocumentRecord> findDocsWithWord(List<String> normalForms) {

        Map<Integer, IndexDocumentRecord> resultDocs = new HashMap<>();

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
            try (DataInputStream termReader = new DataInputStream(new BufferedInputStream(new FileInputStream(Constants.TERM_DICTIONARY_PATH)));
                DataInputStream titleReader = new DataInputStream(new BufferedInputStream(new FileInputStream(Constants.DOCS_TITLES_PATH)));
                DataInputStream linkReader = new DataInputStream(new BufferedInputStream(new FileInputStream(Constants.INDEX_LINKS_PATH)))) {
                System.out.println("start loading");
                while(termReader.available() > 0){
                    termDictionary.put(termReader.readUTF(),termReader.readInt());
                }
                System.out.println(termDictionary.size());
                System.out.println("terms loaded");
                while(titleReader.available() > 0){
                    docsTitleMap.put(titleReader.readInt(),titleReader.readUTF());
                }
                System.out.println("titles loaded");
                while(linkReader.available() > 0){
                    termIndexLinks.put(linkReader.readInt(),linkReader.readLong());
                }
                System.out.println("links loaded");
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
