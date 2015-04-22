/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.bigdata.ganglia;

import java.util.Iterator;

/**
 * A read-only view of the soft state of the cluster as maintained through the
 * ganglia protocol.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IGangliaState {

    /**
     * Return the name of this host.
     */
    public String getHostName();

    /**
     * Return a snapshot of the known hosts.
     * 
     * @return The names of the known hosts and never <code>null</code>.
     */
    public String[] getKnownHosts();

    /**
     * Return an iterator which will visit all timestamped metrics for the
     * specified host.
     * 
     * @param hostName
     *            The host name.
     * 
     * @return The iterator and never <code>null</code>.
     */
    public Iterator<? extends ITimestampMetricValue> iterator(
            final String hostName);

    /**
     * Return the metadata for the given metric.
     * 
     * @param metricName
     *            The metric name (as it appears in ganglia messages).
     * 
     * @return The metadata for that metric iff it is a known metric and
     *         otherwise <code>null</code>.
     */
    public IGangliaMetadataMessage getMetadata(final String metricName);

    /**
     * Return current {@link TimestampMetricValue} of metric on host
     * (thread-safe).
     * <p>
     * If an {@link IGangliaMetadataMessage} is declared for that name, then
     * either the pre-existing {@link TimestampMetricValue} will be returned
     * -or- a new {@link TimestampMetricValue} will be atomically created and
     * returned.
     * <p>
     * Otherwise, this method will return <code>null</code> since it lacks the
     * necessary information to create an {@link IGangliaMetadataMessage} by
     * itself.
     * 
     * @param hostName
     *            The name of the host.
     * @param metricName
     *            The name of the metric.
     * 
     * @return The current value of the metric on that host -or-
     *         <code>null</code> if there is no {@link IGangliaMetadataMessage}
     *         for that metric.
     */
    public ITimestampMetricValue getMetric(final String hostName,
            final String metricName);

}