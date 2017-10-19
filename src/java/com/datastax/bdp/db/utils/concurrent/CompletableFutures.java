/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.utils.concurrent;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Helper methods to work with {@link CompletableFuture}.
 */
public abstract class CompletableFutures
{
    private CompletableFutures()
    {}

    /**
     * Returns a new {@link CompletableFuture} that is completed when all of the given CompletableFutures complete.
     * <p>
     * This work in the same way than {@link CompletableFuture#allOf(CompletableFuture[])}, but take its arguments
     * as a collection rather than an array.
     *
     * @param futures the collection of CompletableFutures.
     * @return a new CompletableFuture that is completed when all {@code futures} complete.
     */
    public static CompletableFuture<Void> allOf(Collection<CompletableFuture<?>> futures)
    {
        if (futures.isEmpty())
            return CompletableFuture.completedFuture(null);

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    /**
     * Returns a new {@link CompletableFuture} that is completed when all of the given CompletableFutures complete.
     * <p>
     * This work in the same way than {@link CompletableFuture#allOf(CompletableFuture[])}, but take its arguments
     * as a stream rather than an array.
     *
     * @param futures the stream of CompletableFutures.
     * @return a new CompletableFuture that is completed when all {@code futures} complete.
     */
    public static CompletableFuture<Void> allOf(Stream<CompletableFuture<?>> futures)
    {
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }
}
