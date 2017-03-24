package com.dsile.se.utils;

import com.dsile.se.dto.IndexTermRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.lucene.morphology.WrongCharaterException;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class IndexingProcessor {

    private static String dumpPath = Constants.WIKIDUMP_RAW_DIR + "out/";
    private static String blockDir = Constants.WIKIDUMP_RAW_DIR + "blockTempDir/";
    private static String blockName = "block";

    private RussianLuceneMorphology rusmorph;


    private static Pattern docHeaderRegex = Pattern.compile("<doc id=\"(.*)\" url=\"(.*)\" title=\"(.*)\">");
    private static Pattern htmlTagsAndSymbolsRegex = Pattern.compile("(&lt;.*?&gt;|<.*?>|&amp(nbsp)?|&quot|&lt|&gt|[№\\.,()—;:\\[\\]\\{\\}\\*\\%\\\"\\'„“‘’«»\\#\\$\\+\\/\\\\!?…])");
    private static Pattern blockDocPattern = Pattern.compile("(\\d+) \\[(.*)\\]");

    private static int oneBlockTerms = 1000000;

    //структура термID - мап<документ, <список позиций термы>>
    private TreeMap<Integer, HashMap<Integer, LinkedList<Integer>>> indexTmpMap = new TreeMap<>();

    private Integer lastTermId = 0;
    private Integer lastBlockId = 0;
    private HashMap<String, Integer> termDictionary = new HashMap<>();
    private HashMap<Integer,Long> termIndexLinks = new HashMap<>();
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
                                i++;
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


                                if (i % oneBlockTerms == 0) {
                                    System.out.println("start block creating");
                                    lastBlockId++;
                                    FileUtils.write(new File(blockDir + blockName + lastBlockId), indexTmpMap.entrySet().stream().
                                            map(entry -> entry.getKey() + " [" + entry.getValue().entrySet()
                                                    .stream().map(docs -> docs.getKey() + "," + docs.getValue().stream().map(Object::toString).collect(Collectors.joining(",")) ).collect(Collectors.joining("|")) + "]").
                                            collect(Collectors.joining("\n")), "UTF-8");
                                    System.out.println("block" + lastBlockId + " created!");
                                    indexTmpMap.clear();
                                }
                            }
                        }
                    }
                }
                System.out.println("Terms: " + termDictionary.size());
                break;
            }
        }
        lastBlockId++;
        FileUtils.write(new File(blockDir + blockName + lastBlockId), indexTmpMap.entrySet().stream().
                map(entry -> entry.getKey() + " [" + entry.getValue().entrySet()
                        .stream().map(docs -> docs.getKey() + "," + docs.getValue().stream().map(Object::toString).collect(Collectors.joining(",")) ).collect(Collectors.joining("|")) + "]").
                collect(Collectors.joining("\n")), "UTF-8");


        FileUtils.write(new File("wordCounts.txt"), wordsRates.entrySet().stream().
                map(entry -> entry.getKey() + " " + entry.getValue().toString()).
                collect(Collectors.joining("\n")), "UTF-8");

        //TODO: Serializing fucking slow - better make standard byte writing
        try(ObjectOutputStream termToFileWriter = new ObjectOutputStream(new FileOutputStream(new File(Constants.TERM_DICTIONARY_PATH)))){
            termToFileWriter.writeObject(termDictionary);
        }

        try(ObjectOutputStream titlesToFileWriter = new ObjectOutputStream(new FileOutputStream(new File(Constants.DOCS_TITLES_PATH)))){
            titlesToFileWriter.writeObject(docsTitleMap);
        }

        indexTmpMap.clear();
        System.out.println(termDictionary.size());
    }


    public void mergeFiles() throws IOException {
        List<File> blocks = new ArrayList<>(FileUtils.listFiles(new File(blockDir), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE));

        List<BufferedReader> brList = new ArrayList<>();
        List<String> lines = new ArrayList<>();
        List<Matcher> matchers = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();

        for (int i = 0; i < blocks.size(); i++) {
            brList.add(new BufferedReader(new FileReader(blocks.get(i))));
            lines.add(brList.get(i).readLine());
            matchers.add(null);
            ids.add(0);
        }

        try(RandomAccessFile memoryMappedFile = new RandomAccessFile(Constants.RESULT_INDEX_PATH, "rw");
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(Constants.RESULT_INDEX_PATH))));
            ObjectOutputStream indexLinksOutput = new ObjectOutputStream(new FileOutputStream(new File(Constants.INDEX_LINKS_PATH)))) {

            long currentByte = 0;

            while (true) {
                boolean allLinesNull = true;
                for (int i = 0; i < lines.size(); i++) {
                    if (lines.get(i) != null) {
                        matchers.set(i, blockDocPattern.matcher(lines.get(i)));
                        matchers.get(i).matches();
                        ids.set(i, Integer.parseInt(matchers.get(i).group(1)));
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
                TreeSet<IndexTermRecord> resultSet = new TreeSet<>();
                for (int indexWithMinId : indexesWithMinId) {
                    String[] docPairs = matchers.get(indexWithMinId).group(2).split("\\|");
                    for(String s : docPairs){
                        String[] docMeta = s.split(",");
                        resultSet.add(new IndexTermRecord(Arrays.stream(docMeta).map(Integer::parseInt).collect(Collectors.toList())));
                    }
                }

                double IDF = Math.log(((double)docsTitleMap.size())/resultSet.size());

                int sumOfPositionSizes = resultSet.stream().mapToInt(IndexTermRecord::getPositionsSize).sum();
                int bufferSize = (Integer.BYTES + resultSet.size() * (Integer.BYTES + Float.BYTES + Integer.BYTES) + Integer.BYTES * sumOfPositionSizes);

                System.out.println(bufferSize);

                out.writeInt(bufferSize - Integer.BYTES);

                for(IndexTermRecord e : resultSet){
                    out.writeInt(e.getDocId());
                    out.writeFloat((float)(IDF * e.calculateIf()));
                    out.writeInt(e.getPositionsSize());
                    for(int position : e.getPositions()){
                        out.writeInt(position);
                    }
                }

                termIndexLinks.put(minId, currentByte);

                currentByte += bufferSize;

                for (int indexWithMinId : indexesWithMinId) {
                    lines.set(indexWithMinId, brList.get(indexWithMinId).readLine());
                }
            }

            indexLinksOutput.writeObject(termIndexLinks);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        IndexingProcessor processor = new IndexingProcessor();
        processor.clearBlocksAndIndexes();
        processor.process();
        processor.mergeFiles();
    }

}
