/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.IndexManagerCallable;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.journal.jini.ha.HAClient.HAConnection;
import com.bigdata.journal.jini.ha.HALogIndex.IHALogRecord;
import com.bigdata.journal.jini.ha.SnapshotIndex.ISnapshotRecord;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.zk.ZKQuorumClient;
import com.bigdata.util.BytesUtil;

import cutthecrap.utils.striterators.EmptyIterator;

/**
 * Accesses Zookeeper state and then connects to each service member to
 * retrieve log digest information.
 * 
 * This task will return an object with the transaction reference to be released
 * and the range of commit counters for the logs to be checked.
 * 
 * Then tasks should be submitted for each service for a subset of the total logs available.
 * 
 * Each subset is processed and a subset summary created.
 * 
 * The dump method returns an Iterator providing the option to block on hasNext.  This handles
 * the batching of log computations for each service, the idea being that for large
 * numbers of logs comparisons could be made across a subset of the logs.
 * 
 * An intermediate ServiceLogWait class wraps the future to allow an iterator to
 * block on get() before returning the wrapped values as ServiceLogs instances.
 * 
 * Internal to each service the "serviceThreads" value defines the number of concurrent
 * log digest computations at any point.
 * 
 * @author Martyn Cutcher
 *
 */
public class DumpLogDigests {

    private static final Logger log = Logger.getLogger(DumpLogDigests.class);
    
	private static final int DEFAULT_SERVICE_THREADS = 5;
	private static final int DEFAULT_BATCH = 50;

	final HAClient client;

	/**
	 * Just needs the configuration file for the discovery service and the local
	 * zookeeper port
	 */
	public DumpLogDigests(final String[] configFiles)
			throws ConfigurationException, IOException, InterruptedException {

		// Sleeping should not be necessary
		// Thread.sleep(1000); // wait for zk services to register!

		client = new HAClient(configFiles);
	}
	
	public void shutdown() {
		client.disconnect(true);
	}


    
    public Iterator<ServiceLogs> summary(final String serviceRoot) throws IOException, ExecutionException {
    	return summary(dump(serviceRoot, DEFAULT_BATCH, DEFAULT_SERVICE_THREADS));
    }
    
    public Iterator<ServiceLogs> dump(final String serviceRoot) throws IOException, ExecutionException {
    	return dump(serviceRoot, DEFAULT_BATCH, DEFAULT_SERVICE_THREADS);
    }
    
	public Iterator<ServiceLogs> dump(final String serviceRoot,
			final int batchSize, final int serviceThreads) throws IOException,
			ExecutionException {
		try {
			// wait for zk services to register should no longer be necessary
			// Thread.sleep(1000);

			// retrieve a list of joined services
			List<HAGlue> services = services(serviceRoot);

			if (services.isEmpty())
				throw new IllegalArgumentException("No services found for "
						+ serviceRoot);

			// Retrieve a LogDigestParmas for each service with a PinLogs task
			// Retrieve in sequential order and use a try finally pattern to
			// invoke UnpinLogs for everything that was pinned.
			//
			// Note that the new accessor pattern is used to increment the 
			//	access count in GetLogInfo so this ensures that while concurrent
			//	digest tasks are running the logs will not be removed.
			final ArrayList<HAGlue> pinners = new ArrayList<HAGlue>();
			try {

				long startCC = -1;
				long endCC = -1;
				
				for (HAGlue pinner : services) {
					final LogDigestParams params = pinner.submit(new PinLogs(),
							false).get();
	
					if (log.isInfoEnabled())
						log.info("Pinning startCC: " + params.startCC + ", endCC: "
								+ params.endCC + ", last snapshot: "
								+ params.snapshotCC);
					
					if (params.startCC != -1) { // there are logs available
						if (startCC == -1 || startCC > params.startCC){
							startCC = params.startCC;
						}
						if (endCC < params.endCC){
							endCC = params.endCC;
						}
					}
					
					// only added if PinLogs is successful
					pinners.add(pinner); // add as pinner to be unpinned later
				}
				
				/**
				 * Now access serviceIDs so that we can use discovery to gain
				 * HAGlue interface.
				 * 
				 * Submit all requests for concurrent processing, then add
				 * results
				 */
				List<Future<List<HALogInfo>>> results = new ArrayList<Future<List<HALogInfo>>>();
				long batchStart = startCC;
				long batchEnd = batchStart + batchSize - 1;
				int tasks = 0;
				while (true) {
					if (batchEnd > endCC)
						batchEnd = endCC;

					if (log.isInfoEnabled())
						log.info("Running batch start: " + batchStart
								+ ", end: " + batchEnd + " across " + services);

					for (final HAGlue glue : services) {

						results.add(glue.submit(new GetLogInfo(batchStart,
								batchEnd, serviceThreads), false));

						tasks++;
					}

					if (batchEnd == endCC)
						break;

					batchStart += batchSize;
					batchEnd += batchSize;
				}

				final ArrayList<ServiceLogWait> logs = new ArrayList<ServiceLogWait>();
				for (int t = 0; t < tasks; t++) {
					final int s = t % services.size();
					logs.add(new ServiceLogWait(services.get(s)
							.getServiceUUID().toString(), results.get(t), s,
							services.size()));
				}

				// return an Iterator blocking on the Future value of the next
				// source item before
				// creating a return value
				return new Iterator<ServiceLogs>() {
					final Iterator<ServiceLogWait> src = logs.iterator();

					@Override
					public boolean hasNext() {
						return src.hasNext();
					}

					@Override
					public ServiceLogs next() {
						final ServiceLogWait data = src.next();

						try {
							// This will block on the future.get()
							return new ServiceLogs(data.service,
									data.waitlogInfos.get(), data.item,
									data.batch);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						} catch (ExecutionException e) {
							throw new RuntimeException(e);
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};

			} finally {
				for (final HAGlue pinner : pinners) {
					try {
						pinner.submit(new UnpinLogs(), false);
					} catch (Throwable t) {
						log.error("Problem submitting UnpinLogs", t);
					}
				}
				
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}
    
    /**
     * LogDigestParams with PinLogs and UnpinLogs tasks ensure that
     * transactions (and logs?) will not be removed while the digests
     * are computed.
     * <p>
     * In fact creating a transaction does not protect the halog files,
     * which are now protected from removal during a digest computation
     * by a protectDigest call on the HALogNexus in GetLogInfo.
     * <p>
     * TODO: The read transaction is currently retained but should be removed
     * if/when it becomes clear that it has no use.
     */
    @SuppressWarnings("serial")
	static public class LogDigestParams implements Serializable {
    	final public long startCC;
    	final public long endCC;
    	final public long snapshotCC;
    	
    	LogDigestParams(final long startCC, final long endCC, long sscc) {
    		this.startCC = startCC;
    		this.endCC = endCC;
    		this.snapshotCC = sscc;
    	}
    }
    
    @SuppressWarnings("serial")
	static class PinLogs extends IndexManagerCallable<LogDigestParams> {

		@Override
		public LogDigestParams call() throws Exception {
			final HAJournal ha = (HAJournal) this.getIndexManager();
			
			final HALogNexus nexus = ha.getHALogNexus();
			Iterator<IHALogRecord> logs = nexus.getHALogs();
			final long startCC;
			long endCC = nexus.getCommitCounter()+1; // endCC
			if (logs.hasNext()) {
				startCC = logs.next().getCommitCounter();
			} else {
				startCC = endCC;
			}
			
			final SnapshotManager ssmgr = ha.getSnapshotManager();
			final ISnapshotRecord rec = ssmgr.getNewestSnapshot();
			final long sscc = rec != null ? rec.getCommitCounter() : -1;
			
			nexus.addAccessor();
			
			// return new LogDigestParams(tx, startCC-3, endCC+3); // try asking for more logs than available
			 return new LogDigestParams(startCC, endCC, sscc);
		}
    	
    }
    
    @SuppressWarnings("serial")
	static class UnpinLogs extends IndexManagerCallable<Void> {

    	@Override
		public Void call() throws Exception {
			final HAJournal ha = (HAJournal) this.getIndexManager();
			
			ha.getHALogNexus().releaseAccessor();
			
			return null;
		}
    	
    }
    
    /**
     * The GetLogInfo callable is submitted to each service, retrieving a
     * List of HALogInfo data elements for each commit counter log
     * requested.
     * <p>
     * It is parameterized for start and end commit counters for the logs and
     * the number of serviceThreads to split the digest computations across.
     */
    
    @SuppressWarnings("serial")
	static class GetLogInfo extends IndexManagerCallable<List<HALogInfo>> {
    private final long endCC;
    private final long startCC;
    private final int serviceThreads;

	GetLogInfo(final long startCC, final long endCC, final int serviceThreads) {
    		this.startCC = startCC;
    		this.endCC = endCC;
    		this.serviceThreads = serviceThreads;
    		
    		if (serviceThreads < 1 || serviceThreads > 20) {
    			throw new IllegalArgumentException();
    		}
    	}
    	
		@Override
		public List<HALogInfo> call() throws Exception {
			final ConcurrentSkipListSet<HALogInfo> infos = new ConcurrentSkipListSet<HALogInfo>();
			
			HAJournal ha = (HAJournal) this.getIndexManager();
						
			final HALogNexus nexus = ha.getHALogNexus();
			nexus.addAccessor();
			try {
				long openCC = nexus.getCommitCounter();
				log.warn("Open Commit Counter: " + openCC + ", startCC: "
						+ startCC + ", endCC: " + endCC);

				/**
				 * Submit each computation as task to pooled executor service -
				 * say maximum of five threads
				 */
				final ThreadPoolExecutor es = (ThreadPoolExecutor) Executors
						.newFixedThreadPool(serviceThreads);

				final List<Future<Void>> results = new ArrayList<Future<Void>>();

				for (long cc = startCC; cc <= endCC; cc++) {
					final long cur = cc;

					final Future<Void> res = es.submit(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							try {
								final File file = nexus.getHALogFile(cur);

								log.warn("Found log file: " + file.getName());

								// compute file digest
                                final MessageDigest digest = MessageDigest
                                        .getInstance("MD5");
								final IHALogReader r = nexus.getReader(cur);
								try {
								    r.computeDigest(digest);
								} finally {
								    // ensure file is closed.
								    r.close();
								}
								infos.add(new HALogInfo(cur, r.isLive(), digest
										.digest()));
							} catch (FileNotFoundException fnf) {
								// permitted
								infos.add(new HALogInfo(cur, false, null /* digest */));
							} catch (Throwable t) {
								log.warn("Unexpected error", t);

								// FIXME: what to do here?
								infos.add(new HALogInfo(cur, false, "ERROR"
										.getBytes()));
							}

							return null;
						}

					});

					results.add(res);
				}

				for (Future<Void> res : results) {
					res.get();
				}

				es.shutdown();

				return new ArrayList<HALogInfo>(infos);
			} finally {
				nexus.releaseAccessor();
			}
		}
    	
    }
    
    @SuppressWarnings("serial")
    /**
     * If the log does not exist then a null digest is defined.
     */
	static public class HALogInfo implements Serializable, Comparable<HALogInfo> {
    	final public long commitCounter;
    	final public boolean isOpen;
    	final public byte[] digest;
    	
    	HALogInfo(final long commitCounter, final boolean isOpen, final byte[] bs) {
    		this.commitCounter = commitCounter;
    		this.isOpen = isOpen;
    		this.digest = bs;
    	}

		@Override
		public int compareTo(HALogInfo other) {
			
			// should not be same commit counter!
			assert commitCounter != other.commitCounter : "self="+this+", other="+other;
			
			return commitCounter < other.commitCounter ? -1 : 1;
		}
		
		public boolean exists() {
			return digest != null;
		}
		
		@Override
		public String toString() {
         return getClass().getName() + "{commitCounter=" + commitCounter
               + ", isOpen=" + isOpen + ", digest="
               + BytesUtil.toHexString(digest) + "}";
		}

    }
    
    /**
     * The ServiceLogs data is a list of digest results for a specific service.
     */
    @SuppressWarnings("serial")
	static public class ServiceLogs implements Serializable {
    	
    	final public List<HALogInfo> logInfos;
    	final public String service;
    	final public int item;
    	final public int batch;
    	
    	ServiceLogs(final String service, final List<HALogInfo> logInfos, final int batch, final int item) {
    		this.logInfos = logInfos;
    		this.service = service;
    		this.item = item;
    		this.batch = batch;
    	}
    	
    	public String toString() {
    		StringBuilder sb = new StringBuilder();
    		sb.append("Service: " + service + "\n");
    		for (HALogInfo li : logInfos) {
    			sb.append("CC[" + li.commitCounter + "]");
    			sb.append(" " + (li.digest == null ? "NOT FOUND" : BytesUtil.toHexString(li.digest)));
    			sb.append(" " + (li.isOpen ? "open" : "closed"));
    			sb.append("\n");
    		}
    		
    		return sb.toString();
    	}
    }
    
    /**
     * The ServiceLogWait supports a delayed iteration where the iterator handles
     * the wait on the Future, rather than the client needing to be aware of the
     * concurrent evaluation.
     */
    class ServiceLogWait {
    	final Future<List<HALogInfo>> waitlogInfos;
    	final String service;
    	final int item;
    	final int batch;
    	
    	ServiceLogWait(final String service, final Future<List<HALogInfo>> waitlogInfos, final int batch, final int item) {
    		this.waitlogInfos = waitlogInfos;
    		this.service = service;
    		this.item = item;
    		this.batch = batch;
    	}
    }
    
    /**
     * There are two required arguments:
     * <li>HA configuration file</li>
     * <li>ServiceRoot</li>
     * <p>
     * A third optional argument 
     * "summary"
     * which, if present, only generates output for HALogInfo that is different for the joined services.
     * <p>
     * The DumpLogDigests dump method returns an Iterator over the halog files returning ServiceLog
     * objects.
     * <p>
     * The command line invocation simply writes a string representation of the returned data to standard out.
     */
    static public void main(final String[] args) throws ConfigurationException, IOException, InterruptedException, ExecutionException {
    	if (args.length < 2 || args.length > 3) {
    		System.err.println("required arguments: <configFile> <serviceRoot> [\"summary\"]");
    		
    		return;
    	}
    	
    	final String configFile = args[0];
    	final String serviceRoot = args[1];
    	
    	final boolean summary = args.length > 2 ? "summary".equals(args[2]) : false;
    	
    	final DumpLogDigests dld = new DumpLogDigests(new String[] {configFile});
    	
    	final Iterator<ServiceLogs> slogs;
    	
    	if (summary) {
    		slogs = dld.summary(serviceRoot);
    	} else {
    		slogs = dld.dump(serviceRoot);
    	}
    	
    	while (slogs.hasNext()) {
    		final ServiceLogs sl = slogs.next();
    		
    		if (sl.logInfos.size() > 0)
    			System.out.println(sl.toString());
    	}
    }
    
    /**
     * Summary will return any reported differences.
     * 
     * @param slogs
     * @return
     */
    static public Iterator<ServiceLogs> summary(final Iterator<ServiceLogs> slogs) {
		return new Iterator<ServiceLogs>() {
			
			Iterator<ServiceLogs> delta = new EmptyIterator<ServiceLogs>();

			@Override
			public boolean hasNext() {
				return delta.hasNext() || slogs.hasNext();
			}

			@Override
			public ServiceLogs next() {
				if (!hasNext())
					throw new NoSuchElementException();
				
				if (delta.hasNext()) {
					return delta.next();
				} else {
					delta = delta(slogs).iterator();
					
					return delta.next();
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
    }
    
    /**
     * Buffers the responses to allow HALogInfo comparisons and remove entries
     * that are similar.
     * 
     * @param slogs
     * @return the service log deltas for a single batch
     */
    static List<ServiceLogs> delta(final Iterator<ServiceLogs> slogs) {
		final ArrayList<ServiceLogs> tmp = new ArrayList<ServiceLogs>();
		
		// retrieve batch results
		while (slogs.hasNext()) {
			ServiceLogs sl = slogs.next(); // will block if not ready
			tmp.add(sl);
			if (sl.item == (sl.batch-1)) // break on last entry for batch
				break;
		}
		
		// size is number of services with batch info
		if (tmp.size() > 0) {
			// select first service to compare with
			final ServiceLogs t = tmp.get(0);
			
			// first check if all others have same number of entries, if not then
			//	do not attempt to compare further.
			for (int n = 1; n < tmp.size(); n++) {
				if (tmp.get(n).logInfos.size() != t.logInfos.size())
					return tmp;
			}
			
			// next compare digests, if all the same then remove the entry
			for (int li = t.logInfos.size()-1; li >= 0; li--) {
				final HALogInfo s = t.logInfos.get(li);
				boolean include = false;
				for (int n = 1; n < tmp.size(); n++) {
					final HALogInfo tst = tmp.get(n).logInfos.get(li);
					if (BytesUtil.compareBytes(tst.digest, s.digest) != 0)
						include = true;
				}
				
				if (!include) { // remove HALogInfo
					for (int n = 0; n < tmp.size(); n++) {
						tmp.get(n).logInfos.remove(li);
					}
				}
			}
		}
		
		return tmp;
    }
 
    private List<HAGlue> services(final String serviceRoot) throws IOException,
            ExecutionException, KeeperException, InterruptedException {

        final List<HAGlue> ret = new ArrayList<HAGlue>();

        final HAConnection cnxn = client.connect();

        final Quorum<HAGlue, ZKQuorumClient<HAGlue>> quorum = cnxn
                .getHAGlueQuorum(serviceRoot);

        final UUID[] uuids = quorum.getJoined();

        final HAGlue[] haglues = cnxn.getHAGlueService(uuids);

        for (HAGlue haglue : haglues) {

            ret.add(haglue);

        }

        client.disconnect(true/* immediate shutdown */);

        return ret;

    }

}
