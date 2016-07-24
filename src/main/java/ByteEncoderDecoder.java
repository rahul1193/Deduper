/**
 * @author rahulanishetty on 23/07/16.
 */
public interface ByteEncoderDecoder<T> {

	byte[] toByteArray(T obj);

	T fromByte(byte[] bytes);
}
