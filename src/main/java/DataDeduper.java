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
    private final ConcurrentHashMap<Integer, Set<byte[]>> hashCodeVsBytesMap = new ConcurrentHashMap<>(SIZE);
    private final AtomicInteger totalDocs = new AtomicInteger(0);
    private final ByteEncoderDecoder<T> encoderDecoder;
    private Map<Integer, Deque<File>> filesMap = new HashMap<>();
    private Map<Integer, Long> docsPerFile = new HashMap<>();
    private int maxFiles = 10;
    private List<File> sortedFiles = new ArrayList<>();
    private final String FILE_PREFIX = UUID.randomUUID().toString();
    private File tempFolderDirectory = new File("/mnt1/tmp");

    private volatile State state = State.ADDING_DOCS;
    private Semaphore semaphore = new Semaphore(1);

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
        int bucket = (Arrays.hashCode(docInBytes) % PRIME) % 31;
        Set<byte[]> docsSet = null;
        docsSet = hashCodeVsBytesMap.get(bucket);
        if (docsSet == null) {
            docsSet = new HashSet<>();
            Set<byte[]> existingDocsSet = hashCodeVsBytesMap.putIfAbsent(bucket, docsSet);
            if (existingDocsSet != null) {
                docsSet = existingDocsSet;
            }
        }
        if (docsSet.contains(docInBytes)) {
            docsSet.add(docInBytes);
            this.totalDocs.incrementAndGet();
        }
    }

    public void complete() {
        if (state == State.COMPLETED) {
            throw new IllegalStateException("complete is called");
        }
        state = State.COMPLETED;
        if (hashCodeVsBytesMap.isEmpty()) {
            return;
        }
        flushDocsToFiles();
        try {
            sortDocsInFiles();
        } catch (Exception e) {
            state = State.ADDING_DOCS;
            throw new RuntimeException(e);
        }
        cleanUpResourcesInternal(false);
    }

    public void cleanupResources() {
        cleanUpResourcesInternal(true);
    }

    public Iterator<List<T>> getIterator(int batchSize) {
        return new DedupDataIterator(batchSize);
    }

    private class DedupDataIterator implements Iterator<List<T>> {
        private final List<T> objects;
        private boolean hasNext = true;
        private ObjectInputStream objectInputStream;
        int currentFileIndex = 0;
        private final int batchSize;

        public DedupDataIterator(int batchSize) {
            this.batchSize = batchSize;
            objects = new ArrayList<>(batchSize);
            initInputStream();
            fetchNext();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public List<T> next() {
            List<T> objects = new ArrayList<>(this.objects);
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
                if (currentFileIndex > sortedFiles.size()) {
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
                objects.clear();
                do {
                    if (!hasNext) {
                        return;
                    }
                    byte[] bytes = (byte[]) objectInputStream.readObject();
                    if (bytes == null || bytes.length == 0) {
                        currentFileIndex++;
                        initInputStream();
                        continue;
                    }
                    objects.add(encoderDecoder.fromByte(bytes));
                } while (++docsAdded < batchSize);

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
                    byte[] bytes = null;
                    do {
                        bytes = (byte[]) objectInputStream.readObject();
                        T t = this.encoderDecoder.fromByte(bytes);
                        docs.add(t);
                    } while (bytes != null);
                }
                try (ObjectOutputStream objectOutputStream = getObjectOutputStreamInternal(file)) {
                    for (T doc : docs) {
                        objectOutputStream.write(encoderDecoder.toByteArray(doc));
                    }
                    objectOutputStream.flush();
                }
            }
        }
        _createUnifiedSortedDocs();
    }

    private ObjectOutputStream getObjectOutputStreamInternal(File file) throws IOException {
        return new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
    }

    private ObjectInputStream getObjectInputStreamInternal(File file) throws IOException {
        return new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
    }

    private void _createUnifiedSortedDocs() throws Exception {
        for (Map.Entry<Integer, Deque<File>> entry : filesMap.entrySet()) {
            File outFile = createTempFile(FILE_PREFIX + "_" + entry.getKey() + "_sorted");
            sortedFiles.add(outFile);
            long docsInserted = 0;
            try (ObjectOutputStream objectOutputStream = getObjectOutputStreamInternal(outFile)) {
                Deque<File> files = entry.getValue();
                List<ObjectInputStream> objectInputStreams = DedupUtils.transformToList(files, new Transformer<File, ObjectInputStream>() {
                    @Override
                    public ObjectInputStream transform(File file) throws Exception {
                        return getObjectInputStreamInternal(file);
                    }
                });
                Map<T, Integer> docsMap = new HashMap<>();
                Set<T> docSet = new TreeSet<>();
                int i = 0;
                for (ObjectInputStream objectInputStream : objectInputStreams) {
                    docSet = getNextDoc(docsMap, docSet, i, objectInputStream);
                    i++;
                }
                while (!docSet.isEmpty()) {
                    T firstDoc = docSet.iterator().next();
                    docSet.remove(firstDoc);
                    int fileIndex = docsMap.get(firstDoc);
                    objectOutputStream.write(encoderDecoder.toByteArray(firstDoc));
                    docsInserted++;
                    if (docsInserted > 10000) {
                        objectOutputStream.flush();
                    }
                    docSet = getNextDoc(docsMap, docSet, fileIndex, objectInputStreams.get(fileIndex));
                }
            }
        }
    }

    private Set<T> getNextDoc(Map<T, Integer> docsMap, Set<T> docSet, int fileIndex, ObjectInputStream objectInputStream) throws IOException, ClassNotFoundException {
        T doc = null;
        do {
            byte[] object = (byte[]) objectInputStream.readObject();
            if (object == null) {
                continue;
            }
            doc = encoderDecoder.fromByte(object);
        } while (docSet.contains(doc));
        docsMap.put(doc, fileIndex);
        return docSet;
    }

    private void acquireLock() {
        semaphore.acquireUninterruptibly();
    }

    private void releaseAllLockedThreads() {
        int permits = Math.abs(semaphore.availablePermits()) + 1;
        semaphore.release(permits);
    }

    private void flushDocsToFiles() {
        String fileName = FILE_PREFIX + "_" + System.nanoTime();
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
                try (ObjectOutputStream objectOutputStream = getObjectOutputStream(entry.getKey(), fileName, totalDocs)) {
                    for (byte[] objectInBytes : objectInBytesSet) {
                        objectOutputStream.write(objectInBytes);
                        totalDocs++;
                    }
                    objectOutputStream.flush();
                }
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
            filesMap.put(fileIndex, files);
        } else if (totalDocs > LIMIT) {
            fileExists = false;
            file = createTempFile(fileName + "_" + fileIndex + "_" + files.size());
            files.add(file);
        } else {
            file = files.getLast();
        }
        return fileExists ? new AppendingOutputStream(new BufferedOutputStream(new FileOutputStream(file))) : getObjectOutputStreamInternal(file);
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

    private enum State {
        FLUSHING, ADDING_DOCS, COMPLETED
    }
}
