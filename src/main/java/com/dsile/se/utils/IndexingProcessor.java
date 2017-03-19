package com.dsile.se.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

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


    private static Pattern docHeaderRegex = Pattern.compile("<doc id=\"(.*)\" url=\"(.*)\" title=\"(.*)\">");
    private static Pattern htmlTagsAndSymbolsRegex = Pattern.compile("(&lt;.*?&gt;|<.*?>|&amp(nbsp)?|&quot|&lt|&gt|[№\\.,()—;:\\[\\]\\{\\}\\*\\%\\\"\\'„“‘’«»\\#\\$\\+\\/\\\\!?…])");
    private static Pattern blockDocPattern = Pattern.compile("(\\d+) \\[(.*)\\]");

    private static int oneBlockTerms = 1000000;

    private TreeMap<Integer, HashMap<Integer, Integer>> indexTmpMap = new TreeMap<>();

    private Integer lastTermId = 0;
    private Integer lastBlockId = 0;
    private HashMap<String, Integer> termDictionary = new HashMap<>();
    private HashMap<Integer,Long> termIndexLinks = new HashMap<>();
    private HashMap<Integer,String> docsTitleMap = new HashMap<>();
    private HashMap<String, Integer> wordsRates = new HashMap<>();
    private HashMap<Integer, Integer> docsWordsCount = new HashMap<>();

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
                            docsWordsCount.put(docId, 0);
                        } else {
                            rawLine = line.replaceAll(htmlTagsAndSymbolsRegex.pattern(), "");
                            for (String word : rawLine.split("[\\u00A0\\s\\uFFFC]+")) {
                                i++;
                                rawWord = word.toLowerCase();
                                if (rawWord.isEmpty()) {
                                    continue;
                                }

                                if (termDictionary.get(rawWord) == null) {
                                    lastTermId++;
                                    termDictionary.put(rawWord, lastTermId);
                                }

                                if(!wordsRates.containsKey(rawWord)){
                                    wordsRates.put(rawWord,1);
                                } else {
                                    wordsRates.put(rawWord, wordsRates.get(rawWord)+1);
                                }


                                docsWordsCount.put(docId, docsWordsCount.get(docId) + 1);

                                Integer termId = termDictionary.get(rawWord);
                                indexTmpMap.computeIfAbsent(termId, k -> new HashMap<>());

                                Integer currentTermInDoc = indexTmpMap.get(termId).get(docId);
                                if(currentTermInDoc != null){
                                    indexTmpMap.get(termId).put(docId,currentTermInDoc+1);
                                } else {
                                    indexTmpMap.get(termId).put(docId,1);
                                }


                                if (i % oneBlockTerms == 0) {
                                    System.out.println("start block creating");
                                    lastBlockId++;
                                    FileUtils.write(new File(blockDir + blockName + lastBlockId), indexTmpMap.entrySet().stream().
                                            map(entry -> entry.getKey() + " [" + entry.getValue().entrySet()
                                                    .stream().map(docs -> docs.getKey() + "," + docs.getValue() ).collect(Collectors.joining("|")) + "]").
                                            collect(Collectors.joining("\n")), "UTF-8");
                                    System.out.println("block" + lastBlockId + " created!");
                                    indexTmpMap.clear();
                                }
                            }
                        }
                    }
                }
                System.out.println("Terms: " + termDictionary.size());
            }
        }
        lastBlockId++;
        FileUtils.write(new File(blockDir + blockName + lastBlockId), indexTmpMap.entrySet().stream().
                map(entry -> entry.getKey() + " [" + entry.getValue().entrySet()
                        .stream().map(docs -> docs.getKey() + "," + docs.getValue() ).collect(Collectors.joining("|")) + "]").
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
            ObjectOutputStream indexLinksOutput = new ObjectOutputStream(new FileOutputStream(new File(Constants.INDEX_LINKS_PATH)))) {

            MappedByteBuffer out;
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

                HashMap<Integer,Float> resultSet = new HashMap<>();
                for (int indexWithMinId : indexesWithMinId) {
                    String[] docPairs = matchers.get(indexWithMinId).group(2).split("\\|");
                    for(String s : docPairs){
                        String[] docMeta = s.split(",");
                        resultSet.put(Integer.parseInt(docMeta[0]),(float)Integer.parseInt(docMeta[1]));
                    }
                }

                double IDF = Math.log(((double)docsTitleMap.size())/resultSet.size());
                resultSet.replaceAll((k,v) -> (float)(IDF * (1 + Math.log(v))));

                int bufferSize = (Integer.BYTES + resultSet.size() * (Integer.BYTES + Float.BYTES));
                out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, currentByte, bufferSize);
                out.putInt(bufferSize - Integer.BYTES);

                for(Map.Entry<Integer, Float> e : resultSet.entrySet()){
                    out.putInt(e.getKey());
                    out.putFloat(e.getValue());
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
