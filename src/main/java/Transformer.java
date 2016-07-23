import java.io.IOException;

/**
 * @author rahulanishetty
 *         on 24/07/16.
 */
public interface Transformer<From, To> {

    To transform(From from) throws Exception;

}
