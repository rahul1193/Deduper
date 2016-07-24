import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * authored by @rahulanishetty on 7/24/16.
 */
public class DeduperTest {

    @Test
    public void testDedup() throws IOException {
        DataDeduper<String> deduper = new DataDeduper<>(new ByteEncoderDecoder<String>() {
            @Override
            public byte[] toByteArray(String obj) {
                return obj.getBytes(StandardCharsets.ISO_8859_1);
            }

            @Override
            public String fromByte(byte[] bytes) {
                return new String(bytes, StandardCharsets.ISO_8859_1);
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
                    deduper.addDoc(data);
                }
            }
        }
        deduper.complete();
        Iterator<List<String>> iterator = deduper.getIterator(10000);
        while (iterator.hasNext()) {
            List<String> next = iterator.next();
            System.out.println(next);
        }
    }
}
