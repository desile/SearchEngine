package com.dsile.se.utils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by desile on 31.03.17.
 */
public class CounterDataOutputStream implements AutoCloseable {

    long written = 0;
    DataOutputStream dataOutputStream;

    public CounterDataOutputStream(DataOutputStream dataOutputStream){
        this.dataOutputStream = dataOutputStream;
    }

    @Override
    public void close() throws IOException {
        dataOutputStream.close();
    }

    public void write(byte[] bytes) throws IOException {
        written += bytes.length;
        dataOutputStream.write(bytes);
    }

    public void writeInt(int num) throws IOException {
        written += Integer.BYTES;
        dataOutputStream.writeInt(num);
    }

    public void writeFloat(float num) throws IOException {
        written += Float.BYTES;
        dataOutputStream.writeFloat(num);
    }

    public long size() {
        return written;
    }
}
