import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * @author rahulanishetty on 23/07/16.
 */
public class AppendingOutputStream extends ObjectOutputStream {

	public AppendingOutputStream(OutputStream out) throws IOException {
		super(out);
	}

	@Override
	protected void writeStreamHeader() throws IOException {
		reset();
	}
}
