package com.deduper;

import com.deduper.encoderdecoder.ByteEncoderDecoder;
import com.deduper.logger.LoggerFactory;
import com.deduper.utils.DedupUtils;
import com.deduper.utils.Transformer;
import org.slf4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author rahulanishetty
 *         on 23/07/16.
 */
public class DataDeduper<T extends Comparable & Serializable> {

    public static final int PRIME = 49999;
    public static final int LIMIT = 100000;
    public static final int SIZE = 31;
    public static final String SUFFIX = ".dedup";
    public static final Logger LOG = LoggerFactory.getLogger(DataDeduper.class);

    private final ConcurrentHashMap<Integer, Set<byte[]>> hashCodeVsBytesMap = new ConcurrentHashMap<>(SIZE);
    private final AtomicInteger totalDocs = new AtomicInteger(0);
    private final ByteEncoderDecoder<T> encoderDecoder;
    private final String FILE_PREFIX = UUID.randomUUID().toString();
    private final Map<File, ObjectOutputStream> objectOutputStreamMap = new HashMap<>();
    private final Semaphore semaphore = new Semaphore(1);
    private volatile State state = State.ADDING_DOCS;
    private Map<Integer, Deque<File>> filesMap = new HashMap<>();
    private Map<Integer, Long> docsPerFile = new HashMap<>();
    private File tempFolderDirectory = new File("/mnt1/tmp");
    private List<File> sortedFiles = new ArrayList<>();
    private int maxFiles = 10;

    public DataDeduper(ByteEncoderDecoder<T> byteEncoderDecoder) {
        this.encoderDecoder = byteEncoderDecoder;
    }

    public DataDeduper(ByteEncoderDecoder<T> byteEncoderDecoder, int maxFiles) {
        if (maxFiles < 1) {
            throw new IllegalStateException("maxfiles cannot be less than 1");
        }
        this.maxFiles = maxFiles > 1 ? maxFiles / 2 : maxFiles;
        this.encoderDecoder = byteEncoderDecoder;
    }

    public DataDeduper(ByteEncoderDecoder<T> byteEncoderDecoder, int maxFiles, String tempFilesFolder) {
        if (maxFiles < 1) {
            throw new IllegalStateException("maxfiles cannot be less than 1");
        }
        this.maxFiles = maxFiles > 1 ? maxFiles / 2 : maxFiles;
        this.encoderDecoder = byteEncoderDecoder;
        tempFolderDirectory = new File(tempFilesFolder);
        if (!tempFolderDirectory.isDirectory()) {
            throw new IllegalStateException("tempFilesFolder is not a directory");
        }
    }

    public void addDoc(T t) {
        if (t == null) {
            return;
        }
        if (state == State.COMPLETED) {
            throw new IllegalStateException("Cannot add docs after complete");
        }
        int totalDocs = this.totalDocs.get();
        synchronized (this) {
            if (totalDocs > LIMIT) {
                state = State.FLUSHING;
            }
        }
        if (state == State.FLUSHING) {
            acquireLock();
        }
        if (this.totalDocs.get() > LIMIT) {
            try {
                flushDocsToFiles();
            } finally {
                releaseAllLockedThreads();
                state = State.ADDING_DOCS;
            }
        }
        byte[] docInBytes = encoderDecoder.toByteArray(t);
        int bucket = Math.abs((Arrays.hashCode(docInBytes) % PRIME) % 31);
        Set<byte[]> docsSet = null;
        docsSet = hashCodeVsBytesMap.get(bucket);
        if (docsSet == null) {
            docsSet = new HashSet<>();
            Set<byte[]> existingDocsSet = hashCodeVsBytesMap.putIfAbsent(bucket, docsSet);
            if (existingDocsSet != null) {
                docsSet = existingDocsSet;
            }
        }
        if (!docsSet.contains(docInBytes)) {
            docsSet.add(docInBytes);
            this.totalDocs.incrementAndGet();
        }
    }

    public void complete() {
        if (state == State.COMPLETED) {
            throw new IllegalStateException("complete is called");
        }
        state = State.COMPLETED;
        flushDocsToFiles();
        cleanUpStreams();
        try {
            sortDocsInFiles();
        } catch (Exception e) {
            state = State.ADDING_DOCS;
            throw new RuntimeException(e);
        }
        cleanUpResourcesInternal(false);
        cleanUpStreams();
    }

    public void cleanupResources() {
        cleanUpResourcesInternal(true);
    }

    public Iterator<List<T>> getIterator(int batchSize) {
        return new DedupDataIterator(batchSize);
    }

    private class DedupDataIterator implements Iterator<List<T>> {
        private List<T> objects;
        private boolean hasNext = true;
        private ObjectInputStream objectInputStream;
        int currentFileIndex = 0;
        private final int batchSize;

        public DedupDataIterator(int batchSize) {
            this.batchSize = batchSize;
            initInputStream();
            fetchNext();
        }

        @Override
        public boolean hasNext() {
            return !(objects == null || objects.isEmpty());
        }

        @Override
        public List<T> next() {
            List<T> objects = Collections.unmodifiableList(this.objects);
            fetchNext();
            return objects;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported");
        }

        private void initInputStream() {
            try {
                if (this.objectInputStream != null) {
                    this.objectInputStream.close();
                }
                if (currentFileIndex >= sortedFiles.size()) {
                    hasNext = false;
                    return;
                }
                File file = sortedFiles.get(currentFileIndex);
                this.objectInputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void fetchNext() {
            try {
                int docsAdded = 0;
                objects = new ArrayList<>(batchSize);
                do {
                    if (!hasNext) {
                        LOG.error("docs added into batch : " + docsAdded);
                        return;
                    }
                    try {
                        //noinspection unchecked
                        objects.add((T) objectInputStream.readObject());
                    } catch (EOFException e) {
                        currentFileIndex++;
                        initInputStream();
                        continue;
                    }
                } while (++docsAdded < batchSize);
                LOG.error("docs added into batch : " + docsAdded);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void sortDocsInFiles() throws Exception {
        for (Map.Entry<Integer, Deque<File>> entry : filesMap.entrySet()) {
            Deque<File> files = entry.getValue();
            if (files == null || files.isEmpty()) {
                continue;
            }
            Iterator<File> iterator = files.iterator();
            while (iterator.hasNext()) {
                File file = iterator.next();
                Set<T> docs = new TreeSet<>();
                try (ObjectInputStream objectInputStream = getObjectInputStreamInternal(file)) {
                    T t;
                    try {
                        //noinspection unchecked
                        while ((t = (T) objectInputStream.readObject()) != null) {
                            docs.add(t);
                        }
                    } catch (EOFException e) {
                        //ignore
                    }
                }
                try (ObjectOutputStream objectOutputStream = getObjectOutputStreamInternal(file, false)) {
                    for (T doc : docs) {
                        objectOutputStream.writeObject(doc);
                    }
                    objectOutputStream.flush();
                    objectOutputStream.reset();
                    objectOutputStreamMap.remove(file);
                }
            }
        }
        _createUnifiedSortedDocs();
    }

    private ObjectOutputStream getObjectOutputStreamInternal(File file, boolean returnAppending) throws IOException {
        if (!returnAppending) {
            objectOutputStreamMap.put(file, new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file))));
        }
        return objectOutputStreamMap.get(file);
    }

    private ObjectInputStream getObjectInputStreamInternal(File file) throws IOException {
        return new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
    }

    private void _createUnifiedSortedDocs() throws Exception {
        for (Map.Entry<Integer, Deque<File>> entry : filesMap.entrySet()) {
            File outFile = createTempFile(FILE_PREFIX + "_" + entry.getKey() + "_sorted");
            sortedFiles.add(outFile);
            long docsInserted = 0;
            ObjectOutputStream objectOutputStream = getObjectOutputStreamInternal(outFile, false);
            Deque<File> files = entry.getValue();
            List<ObjectInputStream> objectInputStreams = DedupUtils.transformToList(files, new Transformer<File, ObjectInputStream>() {
                @Override
                public ObjectInputStream transform(File file) throws Exception {
                    return getObjectInputStreamInternal(file);
                }
            });
            Map<T, Integer> docsMap = new TreeMap<>();
            int i = 0;
            for (ObjectInputStream objectInputStream : objectInputStreams) {
                getNextDoc(docsMap, i, objectInputStream);
                i++;
            }
            while (!docsMap.isEmpty()) {
                T firstDoc = docsMap.keySet().iterator().next();
                int fileIndex = docsMap.remove(firstDoc);
                objectOutputStream.writeObject(firstDoc);
                docsInserted++;
                getNextDoc(docsMap, fileIndex, objectInputStreams.get(fileIndex));
            }
            for (ObjectInputStream objectInputStream : objectInputStreams) {
                objectInputStream.close();
            }
            LOG.error("docs inserted for file " + docsInserted);
        }
    }

    private void getNextDoc(Map<T, Integer> docsMap, int fileIndex, ObjectInputStream objectInputStream) throws IOException, ClassNotFoundException {
        T doc = null;
        do {
            try {
                //noinspection unchecked
                doc = (T) objectInputStream.readObject();
            } catch (EOFException e) {
                return;
            }
        } while (docsMap.containsKey(doc));
        docsMap.put(doc, fileIndex);
    }

    private void acquireLock() {
        semaphore.acquireUninterruptibly();
    }

    private void releaseAllLockedThreads() {
        int permits = Math.abs(semaphore.availablePermits()) + 1;
        semaphore.release(permits);
    }

    private void flushDocsToFiles() {
        String fileName = FILE_PREFIX + "_";
        for (Map.Entry<Integer, Set<byte[]>> entry : hashCodeVsBytesMap.entrySet()) {
            Set<byte[]> objectInBytesSet = entry.getValue();
            if (objectInBytesSet == null || objectInBytesSet.isEmpty()) {
                continue;
            }
            try {
                int fileIndex = entry.getKey() % maxFiles;
                Long totalDocs = docsPerFile.get(fileIndex);
                if (totalDocs == null) {
                    totalDocs = 0L;
                }
                ObjectOutputStream objectOutputStream = getObjectOutputStream(entry.getKey(), fileName, totalDocs);
                totalDocs = docsPerFile.get(fileIndex);
                for (byte[] objectInBytes : objectInBytesSet) {
                    objectOutputStream.writeObject(encoderDecoder.fromByte(objectInBytes));
                    totalDocs++;
                }
                objectOutputStream.flush();
                objectOutputStream.reset();
                docsPerFile.put(fileIndex, totalDocs);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        //set docs to 0 once flush is done
        hashCodeVsBytesMap.clear();
        totalDocs.getAndSet(0);
    }

    private File createTempFile(String fileName) {
        return DedupUtils.createTempFile(fileName, SUFFIX, this.tempFolderDirectory);
    }

    private ObjectOutputStream getObjectOutputStream(int index, String fileName, long totalDocs) throws IOException {
        int fileIndex = index % maxFiles;
        Deque<File> files = filesMap.get(fileIndex);
        boolean fileExists = true;
        File file = null;
        if (files == null || files.isEmpty()) {
            fileExists = false;
            file = createTempFile(fileName + "_" + fileIndex);
            files = new ArrayDeque<>();
            files.add(file);
            docsPerFile.put(index % maxFiles, 0L);
            filesMap.put(fileIndex, files);
        } else if (totalDocs > LIMIT) {
            fileExists = false;
            file = createTempFile(fileName + "_" + fileIndex + "_" + files.size());
            docsPerFile.put(index % maxFiles, 0L);
            files.add(file);
        } else {
            file = files.getLast();
        }
        return getObjectOutputStreamInternal(file, fileExists);
    }

    private void cleanUpResourcesInternal(boolean deleteSortedFiles) {
        state = State.COMPLETED;
        for (Deque<File> files : filesMap.values()) {
            for (File file : files) {
                DedupUtils.deleteFile(file);
            }
        }
        if (deleteSortedFiles) {
            if (sortedFiles != null && !sortedFiles.isEmpty()) {
                for (File sortedFile : sortedFiles) {
                    DedupUtils.deleteFile(sortedFile);
                }

            }
        }
    }

    private void cleanUpStreams() {
        for (Map.Entry<File, ObjectOutputStream> entry : this.objectOutputStreamMap.entrySet()) {
            ObjectOutputStream stream = entry.getValue();
            try {
                stream.flush();
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.objectOutputStreamMap.clear();
    }

    private enum State {
        FLUSHING, ADDING_DOCS, COMPLETED
    }
}
