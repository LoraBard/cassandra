/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.metrics;

import com.codahale.metrics.Snapshot;

/**
 * A histogram class which is an aggregation of other histograms with the same
 * max trackable value and zero consideration, i.e. the same bucket offsets.
 */
class CompositeHistogram implements Histogram
{
    private final Reservoir reservoir;

    public CompositeHistogram(Reservoir reservoir)
    {
        this.reservoir = reservoir;
    }

    public boolean considerZeroes()
    {
        return reservoir.considerZeroes();
    }

    public long maxTrackableValue()
    {
        return reservoir.maxTrackableValue();
    }

    public void update(long value)
    {
        throw new UnsupportedOperationException("Composite histograms are read-only");
    }

    public long getCount()
    {
        return reservoir.getCount();
    }

    public Snapshot getSnapshot()
    {
        return reservoir.getSnapshot();
    }

    public long[] getOffsets()
    {
        return reservoir.getOffsets();
    }

    public void clear()
    {
        throw new UnsupportedOperationException("Composite histograms are read-only");
    }

    public void aggregate()
    {
        throw new UnsupportedOperationException("Composite histograms are read-only");
    }

    public Type getType()
    {
        return Type.COMPOSITE;
    }

    @Override
    public void compose(Histogram histogram)
    {
        if (histogram == null)
            throw new IllegalArgumentException("Histogram cannot be null");

        reservoir.add(histogram);
    }
}