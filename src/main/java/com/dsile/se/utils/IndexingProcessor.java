package com.dsile.se.utils;

import com.dsile.se.VariableByteCode;
import com.dsile.se.dto.IndexDocumentRecord;
import com.dsile.se.dto.IndexTermRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.lucene.morphology.WrongCharaterException;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class IndexingProcessor {

    private static String dumpPath = Constants.WIKIDUMP_RAW_DIR + "out/";
    private static String blockDir = Constants.WIKIDUMP_RAW_DIR + "blockTempDir2/";
    private static String blockName = "block";

    private RussianLuceneMorphology rusmorph;


    private static Pattern docHeaderRegex = Pattern.compile("<doc id=\"(.*)\" url=\"(.*)\" title=\"(.*)\">");
    private static Pattern htmlTagsAndSymbolsRegex = Pattern.compile("(&lt;.*?&gt;|<.*?>|&amp(nbsp)?|&quot|&lt|&gt|[№\\.,()—;:\\[\\]\\{\\}\\*\\%\\\"\\'„“‘’«»\\#\\$\\+\\/\\\\!?…])");
    private static Pattern blockDocPattern = Pattern.compile("(\\d+) \\[(.*)\\]");

    private static int oneBlockFiles = 30;

    //структура термID - мап<документ, <список позиций термы>>
    private TreeMap<Integer, HashMap<Integer, LinkedList<Integer>>> indexTmpMap = new TreeMap<>();

    private Integer lastTermId = 0;
    private Integer lastBlockId = 0;
    private HashMap<String, Integer> termDictionary = new HashMap<>();
    private HashMap<Integer,Long[]> termIndexLinks = new HashMap<>();
    private HashMap<Integer,String> docsTitleMap = new HashMap<>();
    private HashMap<String, Integer> wordsRates = new HashMap<>();

    public IndexingProcessor(){
        try {
            rusmorph = new RussianLuceneMorphology();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clearBlocksAndIndexes() throws IOException {
        for (File block : FileUtils.listFiles(new File(blockDir), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)) {
            if(block.getName().startsWith("block")){
                block.delete();
            }
        }
        Files.deleteIfExists(Paths.get(Constants.RESULT_INDEX_PATH));
        Files.deleteIfExists(Paths.get(Constants.TERM_DICTIONARY_PATH));
        Files.deleteIfExists(Paths.get(Constants.INDEX_LINKS_PATH));
        Files.deleteIfExists(Paths.get(Constants.DOCS_TITLES_PATH));
    }

    public void process() throws IOException {
        Integer i = 0;
        int docWordIterator = 0;
        dirCycle:
        for (File dir : FileUtils.listFilesAndDirs(new File(dumpPath), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)) {
            if (dir.isDirectory() && !dir.getName().equals("out")) {
                System.out.println("DIR: " + dir.getName());
                for (File file : FileUtils.listFiles(dir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)) {
                    System.out.println(file.getName());
                    List<String> fileContent = FileUtils.readLines(file, "UTF-8");
                    Integer docId = -1;
                    String rawLine;
                    String rawWord;
                    for (String line : fileContent) {
                        Matcher matcher = docHeaderRegex.matcher(line);
                        if (matcher.matches()) {
                            docId = Integer.parseInt(matcher.group(1));
                            docsTitleMap.put(docId,matcher.group(3));
                            docWordIterator = 0;
                        } else {
                            rawLine = line.replaceAll(htmlTagsAndSymbolsRegex.pattern(), "");
                            for (String word : rawLine.split("[\\u00A0\\s\\uFFFC]+")) {
                                rawWord = word.toLowerCase();
                                if (rawWord.isEmpty()) {
                                    continue;
                                }

                                List<String> normalForms;
                                try{
                                    normalForms = rusmorph.getNormalForms(rawWord);
                                } catch (WrongCharaterException wce){
                                    normalForms = Collections.singletonList(rawWord);
                                }

                                docWordIterator++;

                                //In case of several normal forms of word we will write it all to index
                                //may be it will be better if we will take and write only one?
                                for(String token : normalForms){

                                    if (termDictionary.get(token) == null) {
                                        lastTermId++;
                                        termDictionary.put(token, lastTermId);
                                    }

                                    if(!wordsRates.containsKey(token)){
                                        wordsRates.put(token,1);
                                    } else {
                                        wordsRates.put(token, wordsRates.get(token)+1);
                                    }

                                    Integer termId = termDictionary.get(token);
                                    indexTmpMap.computeIfAbsent(termId, k -> new HashMap<>());

                                    if(!indexTmpMap.get(termId).containsKey(docId)){
                                        indexTmpMap.get(termId).put(docId, new LinkedList<>());
                                    }

                                    indexTmpMap.get(termId).get(docId).push(docWordIterator);

                                }
                            }
                        }
                    }
                    i++;
                    if (i % oneBlockFiles == 0) {
                        System.out.println("start block creating");
                        lastBlockId++;
                        try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(blockDir + blockName + lastBlockId))))){
                            for(Map.Entry<Integer, HashMap<Integer, LinkedList<Integer>>> entry : indexTmpMap.entrySet()){
                                out.writeInt(entry.getKey());
                                out.writeInt(entry.getValue().size());
                                for(Map.Entry<Integer, LinkedList<Integer>> documents : entry.getValue().entrySet()){
                                    out.writeInt(documents.getKey());
                                    out.writeInt(documents.getValue().size());
                                    for(int position : documents.getValue()){
                                        out.writeInt(position);
                                    }
                                }
                            }
                        }
                        System.out.println("block" + lastBlockId + " created!");
                        indexTmpMap.clear();
                    }
                }
                System.out.println("Terms: " + termDictionary.size());
                //break dirCycle;
            }
        }
        lastBlockId++;



        try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(blockDir + blockName + lastBlockId))))){
            for(Map.Entry<Integer, HashMap<Integer, LinkedList<Integer>>> entry : indexTmpMap.entrySet()){
                out.writeInt(entry.getKey());
                out.writeInt(entry.getValue().size());
                for(Map.Entry<Integer, LinkedList<Integer>> documents : entry.getValue().entrySet()){
                    out.writeInt(documents.getKey());
                    out.writeInt(documents.getValue().size());
                    for(int position : documents.getValue()){
                        out.writeInt(position);
                    }
                }
            }
        }


        FileUtils.write(new File("wordCounts.txt"), wordsRates.entrySet().stream().
                map(entry -> entry.getKey() + " " + entry.getValue().toString()).
                collect(Collectors.joining("\n")), "UTF-8");

        try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Constants.TERM_DICTIONARY_PATH))))) {
            for(Map.Entry<String,Integer> e : termDictionary.entrySet()){
                out.writeUTF(e.getKey());
                out.writeInt(e.getValue());
            }
        }

        try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Constants.DOCS_TITLES_PATH))))) {
            for(Map.Entry<Integer,String> e : docsTitleMap.entrySet()){
                out.writeInt(e.getKey());
                out.writeUTF(e.getValue());
            }
        }

        indexTmpMap.clear();
        System.out.println(termDictionary.size());
    }


    public void mergeFiles() throws IOException {
        List<File> blocks = new ArrayList<>(FileUtils.listFiles(new File(blockDir), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE));

        List<DataInputStream> brList = new ArrayList<>();
        List<IndexTermRecord> lines = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();

        int minimalGapListSize = 50;

        for (int i = 0; i < blocks.size(); i++) {
            brList.add(new DataInputStream(new BufferedInputStream(new FileInputStream(blocks.get(i)))));
            DataInputStream curIntput = brList.get(i);
            if(curIntput.available() == 0){
                lines.add(null);
                ids.add(0);
                continue;
            }
            IndexTermRecord termRecord = new IndexTermRecord(curIntput.readInt());
            int docsSize = curIntput.readInt();
            for(int j = 0; j < docsSize; j++){
                int docId = curIntput.readInt();
                int positionsSize = curIntput.readInt();
                List<Integer> positions = new LinkedList<>();
                for(int k = 0; k < positionsSize; k++){
                    positions.add(curIntput.readInt());
                }
                positions.sort(Integer::compareTo);
                int prevPos = 0;
                for(int k = 0; k < positionsSize; k++){
                    positions.set(k,positions.get(k) - prevPos);
                    prevPos = positions.get(k) + prevPos;
                }
                termRecord.addDocuments(new IndexDocumentRecord(docId,0,positions));
            }
            lines.add(termRecord);
            ids.add(0);
        }

        try(CounterDataOutputStream out = new CounterDataOutputStream(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Constants.RESULT_INDEX_PATH)))));
            DataOutputStream linksOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Constants.INDEX_LINKS_PATH))))) {

            long currentByte = 0;
            int debugCycle = 0;

            while (true) {
                debugCycle++;
                if(debugCycle % 1000 == 0){
                    System.out.println(debugCycle);
                }
                boolean allLinesNull = true;
                for (int i = 0; i < lines.size(); i++) {
                    if (lines.get(i) != null) {
                        ids.set(i, lines.get(i).getTermId());
                        allLinesNull = false;
                    } else {
                        ids.set(i, Integer.MAX_VALUE);
                    }
                }

                if (allLinesNull) {
                    break;
                }

                int minId = ids.stream().min(Integer::compareTo).get();
                List<Integer> indexesWithMinId = new LinkedList<>();
                for (int i = 0; i < ids.size(); i++) {
                    if (ids.get(i) == minId) {
                        indexesWithMinId.add(i);
                    }
                }


                //структура docId, tfIdf, list<positions>
                List<IndexDocumentRecord> resultSet = new ArrayList<>();
                for (int indexWithMinId : indexesWithMinId) {
                    resultSet.addAll(lines.get(indexWithMinId).getDocuments());
                }

                resultSet.sort(Collections.reverseOrder());

                double IDF = Math.log(((double)1_300_000)/resultSet.size());
                int gapSize = (int)(Math.sqrt(resultSet.size()));

                long bufferSize = out.size();

                //out.writeInt(bufferSize - Integer.BYTES);
                out.writeInt(resultSet.size());

                int countGaps = 0;

                //for VB compression
                int previousDocId = 0;
                int deltaDocId = 0;

                for(int k = 0; k < resultSet.size(); k++){
                    if(minimalGapListSize <= resultSet.size() && k % gapSize == 0){
                        if (k + gapSize < resultSet.size()) {
                            out.write(VariableByteCode.encodeNumber(resultSet.get(k).getDocId()));
                            previousDocId = resultSet.get(k).getDocId();
                            int gapBytes = 0;
                            int prevDocId = previousDocId;
                            for (int m = k; m < k + gapSize; m++) {
                                deltaDocId = resultSet.get(m+1).getDocId() - prevDocId;
                                gapBytes += Float.BYTES + VariableByteCode.encodeNumber(VariableByteCode.encode(resultSet.get(m).getPositions()).length).length + VariableByteCode.encode(resultSet.get(m).getPositions()).length + VariableByteCode.encodeNumber(deltaDocId).length;
                                prevDocId = resultSet.get(m+1).getDocId();
                            }
                            countGaps++;
                            out.write(VariableByteCode.encodeNumber(gapBytes - VariableByteCode.encodeNumber(deltaDocId).length));//исключаем последний docid - так как перепрыгнуть нам нужно к нему
                        } else {
                            out.write(VariableByteCode.encodeNumber(resultSet.get(k).getDocId()));
                            previousDocId = resultSet.get(k).getDocId();
                        }
                    } else {
                        deltaDocId = resultSet.get(k).getDocId() - previousDocId;
                        out.write(VariableByteCode.encodeNumber(deltaDocId));
                        previousDocId = resultSet.get(k).getDocId();
                    }
                    out.writeFloat((float)(IDF * resultSet.get(k).calculateIf()));
                    out.write(VariableByteCode.encodeNumber(VariableByteCode.encode(resultSet.get(k).getPositions()).length));
                    for(int i = 0; i < resultSet.get(k).getPositionsSize(); i++ ){//обратный обход для восстановления порядка возрастания позиций
                        out.write(VariableByteCode.encodeNumber(resultSet.get(k).getPositions().get(i)));
                    }
                }

                bufferSize = out.size() - bufferSize;
                termIndexLinks.put(minId, new Long[]{currentByte,bufferSize});

                currentByte += bufferSize;

                for (int indexWithMinId : indexesWithMinId) {
                    DataInputStream curIntput = brList.get(indexWithMinId);
                    if(curIntput.available() == 0){
                        lines.set(indexWithMinId,null);
                        continue;
                    }
                    IndexTermRecord termRecord = new IndexTermRecord(curIntput.readInt());
                    int docsSize = curIntput.readInt();
                    for(int j = 0; j < docsSize; j++){
                        int docId = curIntput.readInt();
                        int positionsSize = curIntput.readInt();
                        List<Integer> positions = new LinkedList<>();
                        for(int k = 0; k < positionsSize; k++){
                            positions.add(curIntput.readInt());
                        }
                        positions.sort(Integer::compareTo);
                        int prevPos = 0;
                        for(int k = 0; k < positionsSize; k++){
                            positions.set(k,positions.get(k) - prevPos);
                            prevPos = positions.get(k) + prevPos;
                        }
                        termRecord.addDocuments(new IndexDocumentRecord(docId,0,positions));
                    }
                    lines.set(indexWithMinId, termRecord);
                }
            }

            for(Map.Entry<Integer,Long[]> e : termIndexLinks.entrySet()){
                linksOut.writeInt(e.getKey());
                linksOut.writeLong(e.getValue()[0]);
                linksOut.writeLong(e.getValue()[1]);
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        IndexingProcessor processor = new IndexingProcessor();
        //processor.clearBlocksAndIndexes();
        //processor.process();
        processor.mergeFiles();
    }

}
