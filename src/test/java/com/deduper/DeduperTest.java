package com.deduper;

import com.deduper.DataDeduper;
import com.deduper.encoderdecoder.ByteEncoderDecoder;
import com.deduper.wrapper.StringWrapper;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * authored by @rahulanishetty on 7/24/16.
 */
public class DeduperTest {

    @Test
    public void testDedup() throws IOException {
        DataDeduper<StringWrapper> deduper = new DataDeduper<>(new ByteEncoderDecoder<StringWrapper>() {
            @Override
            public byte[] toByteArray(StringWrapper obj) {
                return obj.getActual().getBytes(StandardCharsets.ISO_8859_1);
            }

            @Override
            public StringWrapper fromByte(byte[] bytes) {
                return new StringWrapper(new String(bytes, StandardCharsets.ISO_8859_1));
            }
        }, 10, "/Users/rahulanishetty/Dev/Deduper/src/test/resources/processed/");
        File file = new File("/Users/rahulanishetty/Dev/Deduper/src/test/resources");
        for (File tempFile : file.listFiles()) {
            if (tempFile.isDirectory() || tempFile.isHidden()) {
                continue;
            }
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(tempFile))) {
                String data = null;
                while ((data = bufferedReader.readLine()) != null) {
                    deduper.addDoc(new StringWrapper(data));
                }
            }
        }
        deduper.complete();
        Iterator<List<StringWrapper>> iterator = deduper.getIterator(500000);
        long totalDocs = 0l;
        while (iterator.hasNext()) {
            List<StringWrapper> next = iterator.next();
            totalDocs += next.size();
        }
        System.out.println(totalDocs);
        deduper.cleanupResources();
    }

    @Test
    public void testInMemory() throws IOException {
        File file = new File("/Users/rahulanishetty/Dev/Deduper/src/test/resources");
        Set<StringWrapper> dataSet = new HashSet<>();
        for (File tempFile : file.listFiles()) {
            if (tempFile.isDirectory() || tempFile.isHidden()) {
                continue;
            }
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(tempFile))) {
                String data = null;
                while ((data = bufferedReader.readLine()) != null) {
                    dataSet.add(new StringWrapper(data));
                }
            }
        }
        System.out.println(dataSet.size());
    }

    @Test
    public void testFile() throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream("/Users/rahulanishetty/Dev/Deduper/src/test/resources/processed/e748c9f5-28d0-46fa-863a-0c3a396a0f2c_1469340678753372000_25705725580844215850.dedup"));
        byte[] bytes = null;
        String s = null;
        while ((bytes = (byte[]) objectInputStream.readObject()) != null) {
            System.out.println(new String(bytes));
        }
        objectInputStream.close();
    }
}
