package com.example.flink.helpers;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A functional interface that extends Function and is Serializable,
 * required for Flink's serialization system when using method references
 * or lambdas that need to be serialized.
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}