package com.dsile.se;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by desile on 02.03.17.
 */
public class RandomAccessExample {

    public static void main(String[] args) throws Exception {
        int count = 10;
        int bufferSize = count * (1 + 4);
        RandomAccessFile memoryMappedFile = new RandomAccessFile("bigFile.txt", "rw");
        MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
        for (int i = 0; i < count; i++) {
            out.put((byte) 'A');
            out.putInt(i);
        }

        System.out.println();
        System.out.println(memoryMappedFile.length());

        out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 30, 10);
        System.out.println((char) out.get(0));
        System.out.println(out.getInt(1));
        System.out.println((char) out.get(5));
        System.out.println(out.getInt(6));

        memoryMappedFile.close();

        Map<String,Integer> testMap = new HashMap<>();
        testMap.put("one",1);
        testMap.put("two",2);
        testMap.put("three",3);
        ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream("testObj"));
        objOut.writeObject(testMap);
        objOut.close();

        Map<String,Integer> testMapIn = new HashMap<>();
        ObjectInputStream objIn = new ObjectInputStream(new FileInputStream("testObj"));
        testMapIn = (HashMap<String,Integer>)objIn.readObject();
        System.out.println(testMapIn.toString());
    }

}
