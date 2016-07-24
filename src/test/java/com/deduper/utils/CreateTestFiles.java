package com.deduper.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * authored by @rahulanishetty on 7/24/16.
 */
public class CreateTestFiles {

    public static final String FILE_PREFIX = "000000";
    public static final long LINES_PER_FILE = 1000000L;
    public static final int NUMBER_OF_FILES = 5;

    public static void main(String[] args) throws IOException {
        for (int numberOfFiles = 0; numberOfFiles < NUMBER_OF_FILES; numberOfFiles++) {
            File file = File.createTempFile(FILE_PREFIX + "_" + numberOfFiles, ".txt", new File("/Users/rahulanishetty/Dev/Deduper/src/test/resources/files/"));
            file.setWritable(true);
            writeRandomData(file);
        }
    }

    private static void writeRandomData(File file) throws IOException {
        Random random = new Random();
        long i = 0;
        Set<Long> docs = new HashSet<>();
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file))) {
            while (i < LINES_PER_FILE) {
                bufferedWriter.write(String.valueOf(generateRandomNumber(random, docs)));
                bufferedWriter.newLine();
                i++;
            }
            bufferedWriter.flush();
        }
    }

    private static long generateRandomNumber(Random random, Set<Long> docs) {
        long randomNumber;
        do {
            randomNumber = Math.abs(random.nextLong() % ((long) (5 * LINES_PER_FILE)));
        } while (docs.contains(randomNumber));
        return randomNumber;
    }
}
