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

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * The metadata and metric state for a ganglia service, including the metadata
 * for all known metrics and the metrics for all known metrics on all known
 * hosts.
 */
public class GangliaState implements IGangliaState {
	
	private static final Logger log = Logger.getLogger(GangliaState.class);

	/**
	 * The name of this host.
	 */
	private final String hostName;

	/**
	 * The factory used to create declarations for new metrics.
	 */
	private final IGangliaMetadataFactory metadataFactory;
	
	/**
	 * Canonicalizing map for metric declarations.
	 * 
	 * TODO If this were a weak value map then we could automatically age out
	 * metadata declarations which were no longer in use. However, we have to be
	 * a bit careful since we need to receive and store the metadata
	 * declarations before we can enter a {@link TimestampMetricValue} record
	 * into the map and without the {@link TimestampMetricValue} the
	 * {@link IGangliaMetadataMessage} reference could easily be only weakly
	 * reachable and get finalized. The typical way to handle this is with a
	 * backing hard reference queue using either a fixed capacity or a timeout.
	 */
	private final ConcurrentHashMap<String/* metricName */, IGangliaMetadataMessage/* decl */> metadata;

	/**
	 * The current state for all known hosts.
	 * 
	 * The timestamped value for each known metric together with the metadata
	 * declaration for that metric. This is the basis for reporting metrics
	 * whose values have changed, for reporting metrics whose values have not
	 * changed but for which TMax would otherwise be exceeded, and for sending
	 * out metadata declarations.
	 */
	private final ConcurrentHashMap<String/* hostName */, ConcurrentHashMap<String/* mungedMetricName */, TimestampMetricValue>> knownHosts;

	/**
	 * 
	 * @param hostName
	 *            The name of this host.
	 * @param metadataFactory
	 *            The factory used to create declarations for new metrics.
	 */
	public GangliaState(final String hostName,
			final IGangliaMetadataFactory metadataFactory) {

		if (hostName == null)
			throw new IllegalArgumentException();
		
		if (metadataFactory== null)
			throw new IllegalArgumentException();
		
		this.hostName = hostName;
		
		this.metadataFactory = metadataFactory;
		
		this.metadata = new ConcurrentHashMap<String/*metricName*/, IGangliaMetadataMessage>();
		
		this.knownHosts = new ConcurrentHashMap<String/* hostName */, ConcurrentHashMap<String, TimestampMetricValue>>();

	}

	@Override
    public String getHostName() {
		
		return hostName;
		
	}
	
	/**
	 * The factory used to create declarations for new metrics and resolve
	 * received declarations to objects with a richer behavior (such as value
	 * translation and scaling).
	 */
	public IGangliaMetadataFactory getMetadataFactory() {
		
		return metadataFactory;
		
	}
	
	@Override
    public String[] getKnownHosts() {
		
		return knownHosts.keySet().toArray(new String[0]);
		
	}

	@Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public Iterator<TimestampMetricValue> iterator(final String hostName) {

		assertValidHostName(hostName);

		final ConcurrentHashMap<String, TimestampMetricValue> hostMetrics = knownHosts
				.get(hostName);

		if (hostMetrics == null) {

			return (Iterator) Collections.emptyList().iterator();

		}
		
		return hostMetrics.values().iterator();
		
	}
	
	@Override
    public IGangliaMetadataMessage getMetadata(final String metricName) {
		
		return metadata.get(metricName);
		
	}

	/**
	 * Atomically declare/resolve the metadata for a metric (thread-safe).
	 * 
	 * @param decl
	 *            The declaration.
	 * 
	 * @return Either <i>decl</i>, a version of the declaration which was
	 *         resolved by the
	 *         {@link IGangliaMetadataFactory#resolve(IGangliaMetadataMessage)},
	 *         or the pre-existing declaration for the same metric.
	 */
	public IGangliaMetadataMessage putIfAbsent(
			final IGangliaMetadataMessage decl) {

		final IGangliaMetadataMessage resolved = metadataFactory.resolve(decl);

		if (resolved == null)
			throw new RuntimeException("Resolution error for " + decl);

		if (!decl.getMetricName().equals(resolved.getMetricName())) {
			// Santity check. The metric name should not be changed.
			throw new RuntimeException("Resolution error: decl=" + decl
					+ ", but resolved metricName=" + resolved.getMetricName());
		}
		
		IGangliaMetadataMessage tmp;
		if ((tmp = metadata.putIfAbsent(decl.getMetricName(), resolved)) == null) {

			// Newly declared.
			if (log.isInfoEnabled())
				log.info("declared: " + resolved);
			
			return resolved;

		}

		// Already declared or lost a data race.
		return tmp;

	}
		
	/**
	 * Get the counters for the specified host (thread-safe, atomically
	 * consistent).
	 * <p>
	 * Note: This method is private since the API can not otherwise provide a
	 * guarantee that the metadata declarations are canonical.
	 * 
	 * @param hostName
	 *            The host name.
	 * 
	 * @return The counters for that host and never <code>null</code> (counters
	 *         are registered if they do not exist).
	 */
	private ConcurrentHashMap<String/* mungedMetricName */, TimestampMetricValue> getHostCounters(
			final String hostName) {

		assertValidHostName(hostName);
		
		ConcurrentHashMap<String/* mungedMetricName */, TimestampMetricValue> hostCounters = knownHosts
				.get(hostName);

		if (hostCounters == null) {
			
			hostCounters = new ConcurrentHashMap<String/* mungedMetricName */, TimestampMetricValue>();

			final ConcurrentHashMap<String/* mungedMetricName */, TimestampMetricValue> tmp = knownHosts
					.putIfAbsent(hostName, hostCounters);

			if (tmp != null) {

				// Lost a data race.
				hostCounters = tmp;

			}

		}

		return hostCounters;

	}

	private void assertValidHostName(final String hostName) {

		if(hostName == null)
			throw new IllegalArgumentException();
		
	}

	@Override
    public TimestampMetricValue getMetric(final String hostName,
			final String metricName) {
		
		assertValidHostName(hostName);

		if(metricName == null)
			throw new IllegalArgumentException();

		final IGangliaMetadataMessage decl = getMetadata(metricName);

		if (decl == null) {

			// Metric is not declared.
			return null;

		}

		// Note: never [null].
		final ConcurrentHashMap<String/* mungedMetricName */, TimestampMetricValue> hostCounters = getHostCounters(hostName);

		// Thin wrapper over the decl.
		final TimestampMetricValue tmp = new TimestampMetricValue(decl);

		// Add to map unless already present.
		final TimestampMetricValue old = hostCounters.putIfAbsent(metricName,
				tmp);

		if (old != null) {

			// Return pre-existing value.
			return old;

		}

		if (log.isDebugEnabled())
			log.debug("declared: host=" + hostName + ", decl" + decl);

		return tmp;

	}

	/**
	 * Purge all metrics for any host whose heartbeat has not been updated in at
	 * the last dmax seconds and any counter which has not been updated within
	 * its dmax seconds.
	 * <p>
	 * Note: This method should be invoked periodically to age out hosts and
	 * metrics which have not reported in for a long time.
	 * 
	 * @param dmax
	 *            The global DMax value.
	 */
	public void purgeOldHostsAndMetrics(final int dmax) {

		final String[] knownHosts = getKnownHosts();

		for (String hostName : knownHosts) {

			// Check the host.
			if (!purgeOldHost(hostName, dmax)) {

				// If we did not purge the host, then check its metrics.
				purgeOldMetrics(hostName);

			}

		}

	}
	
	/**
	 * Purge host if heartbeat is older than dmax. If no heartbeat is recorded,
	 * then this looks for the most recent metric from that host and purges the
	 * host if that metric is older than dmax.
	 * 
	 * @param hostName
	 *            The host to be checked.
	 * @param dmax
	 *            The global DMax.
	 * 
	 * @return <code>true</code> if the host was dropped.
	 */
	private boolean purgeOldHost(final String hostName, final int dmax) {

		if (dmax == 0) {
			// Zero means keep forever.
			return false;
		}

		// Lookup counters for that host.
		final ConcurrentHashMap<String/* mungedMetricName */, TimestampMetricValue> hostCounters = knownHosts
				.get(hostName);

		if (hostCounters == null) {
			// Nothing there.
			return false;
		}
		
		// Look for the heartbeat counter first.
		TimestampMetricValue tmv = hostCounters.get("heartbeat");

		// Age of the most recent metric update.
		int minAge = Integer.MAX_VALUE;

		if(tmv != null) {
			
			/*
			 * Use the age of the last heartbeat if we have received a
			 * heartbeat for that host.
			 */
			minAge = tmv.getAge();
			
		} else {

			/*
			 * Probably no heartbeat yet for that host.
			 * 
			 * Note: If we just ignore this host it could cause host to
			 * remain on the books if we never see a heartbeat but some
			 * other counters were reported before it died. This is handled
			 * by scanning all metrics for that host to find the metric
			 * which was most recently updated.
			 */
			
			final Iterator<TimestampMetricValue> itr = hostCounters.values().iterator();
			
			while(itr.hasNext()) {
				
				final TimestampMetricValue tmv2 = itr.next();
				
				final int age = tmv2.getAge();
				
				if(age < minAge) {
					
					minAge = age;

					tmv = tmv2;
					
				}
				
			}
			
		}

		if (minAge > dmax) {

			// This host has not been updated recently. Drop its metrics.
			deleteHost(hostName);

			log.warn("Purged host: " + hostName + ", last update was "
					+ minAge + " seconds ago for "
					+ tmv.getMetadata().getMetricName());

			return true;
			
		}

		return false;
	}

	/**
	 * Scan all metrics for the given host, purging any whose declared DMax has
	 * been exceeded.
	 * 
	 * @param hostName
	 *            The host name.
	 */
	private void purgeOldMetrics(final String hostName) {

		// Lookup counters for that host.
		final ConcurrentHashMap<String/* mungedMetricName */, TimestampMetricValue> hostCounters = knownHosts
				.get(hostName);

		if (hostCounters == null) {
			// Nothing there.
			return;
		}

		final Iterator<TimestampMetricValue> itr = hostCounters.values()
				.iterator();

		while (itr.hasNext()) {

			final TimestampMetricValue tmv = itr.next();

			final int dmax = tmv.getMetadata().getDMax();

			if (dmax == 0) {
				// Zero means keep forever.
				continue;
			}

			final int age = tmv.getAge();

			if (age > dmax) {

				/*
				 * This metric has not been updated recently. Drop it.
				 */
				hostCounters.remove(tmv.getMetadata().getMetricName(), tmv);

				if (log.isInfoEnabled())
					log.info("Purged metric="
							+ tmv.getMetadata().getMetricName() + " for host="
							+ hostName + ", last update was " + age
							+ " seconds ago");

			}

		}

	}

	/**
	 * Drop counters for the named host.
	 * <p>
	 * Note:There may be some interactions if another thread is currently in the
	 * middle of adding a counter for the host. What can happen is that the
	 * other thread gets the counters for the host and then returns the TMV for
	 * the metric while this method concurrently deletes the entry for the host
	 * from knownHosts. This would mean that the other thread has returned a TMV
	 * entry which is not going to remain visible. If there is a problem, it
	 * will be related to what is then done with that TMV entry.
	 * 
	 * @param hostName
	 *            The host name.
	 */
	public void deleteHost(final String hostName) {

		if (hostName == null)
			throw new IllegalArgumentException();

		knownHosts.remove(hostName);

	}

	/**
	 * Reset the soft state, clearing metadata declarations and metric values
	 * for all hosts.
	 */
	public void reset() {

		metadata.clear();

		knownHosts.clear();

	}

}
