import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author rahulanishetty on 23/07/16.
 */
public class DedupUtils {

	public static <To, From> List<To> transformToList(Collection<From> fromCollection,
			Transformer<From, To> transformer) throws Exception {
		if (fromCollection == null || fromCollection.isEmpty()) {
			return Collections.emptyList();
		}
		if (transformer == null) {
			throw new IllegalStateException("trasformer cannot be null");
		}
		List<To> toList = new ArrayList<>();
		for (From from : fromCollection) {
			To transformed = transformer.transform(from);
			if (transformed != null) {
				toList.add(transformed);
			}
		}
		return toList;
	}

	public static File createTempFile(String fileName, String suffix, File directory) {
		File file;
		try {
			file = File.createTempFile(fileName, suffix, directory);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		file.setWritable(true);
		return file;
	}

	public static void deleteFile(File file) {
		if (file == null) {
			return;
		}
		try {
			if (file.exists()) {
				file.delete();
			}
		} catch (Exception e) {

		}
	}

}
