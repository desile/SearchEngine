package com.dsile.se;

import java.nio.ByteBuffer;
import java.util.*;

import static java.lang.Math.log;


public class VariableByteCode {

    public static byte[] encodeNumber(int n) {
        if (n == 0) {
            return new byte[]{0};
        }
        int i = (int) (log(n) / log(128)) + 1;
        byte[] rv = new byte[i];
        int j = i - 1;
        do {
            rv[j--] = (byte) (n % 128);
            n /= 128;
        } while (j >= 0);
        rv[i - 1] += 128;
        return rv;
    }

    public static byte[] encode(List<Integer> numbers) {
        ByteBuffer buf = ByteBuffer.allocate(numbers.size() * (Integer.SIZE / Byte.SIZE));
        for (Integer number : numbers) {
            buf.put(encodeNumber(number));
        }
        buf.flip();
        byte[] rv = new byte[buf.limit()];
        buf.get(rv);
        return rv;
    }

    public static List<Integer> decode(byte[] byteStream) {
        List<Integer> numbers = new ArrayList<Integer>();
        int n = 0;
        for (byte b : byteStream) {
            if ((b & 0xff) < 128) {
                n = 128 * n + b;
            } else {
                int num = (128 * n + ((b - 128) & 0xff));
                numbers.add(num);
                n = 0;
            }
        }
        return numbers;
    }

    public static int decode(ByteBuffer byteBuffer, int pos){
        int n = 0;
        byte b;
        while(byteBuffer.hasRemaining()){
            b = byteBuffer.get(pos++);
            if ((b & 0xff) < 128) {
                n = 128 * n + b;
            } else {
                return (128 * n + ((b - 128) & 0xff));
            }
        }
        return 0;
    }

    public static int decode(ByteBuffer byteBuffer){
        int n = 0;
        byte b;
        while(byteBuffer.hasRemaining()){
            b = byteBuffer.get();
            if ((b & 0xff) < 128) {
                n = 128 * n + b;
            } else {
                return (128 * n + ((b - 128) & 0xff));
            }
        }
        return 0;
    }

}