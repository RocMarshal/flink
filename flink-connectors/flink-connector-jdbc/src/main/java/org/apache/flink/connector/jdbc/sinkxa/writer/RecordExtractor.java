package org.apache.flink.connector.jdbc.sinkxa.writer;

import java.io.Serializable;
import java.util.function.Function;

/**
 * An interface to extract a value from given argument.
 *
 * @param <F> The type of given argument
 * @param <T> The type of the return value
 */
public interface RecordExtractor<F, T> extends Function<F, T>, Serializable {
    static <T> RecordExtractor<T, T> identity() {
        return x -> x;
    }
}
