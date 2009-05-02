/**

 Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

 Contact:
 SYSTAP, LLC
 4501 Tower Road
 Greensboro, NC 27410
 licenses@bigdata.com

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
/*
 * Created on Mar 13, 2007
 */

package com.bigdata.resources;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.cache.ICacheEntry;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.Journal;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.MetadataService;

/**
 * The {@link ResourceManager} has broad responsibility for journal files, index
 * segment files, maintaining index views during overflow processing, and
 * managing the transparent decomposition of scale-out indices and the
 * distribution of the key-range index partitions for those scale-out indidces.
 * <p>
 * This class is implemented in several layers:
 * <dl>
 * <dt>{@link ResourceManager}</dt>
 * <dd>Concrete implementation.</dd>
 * <dt>{@link OverflowManager}</dt>
 * <dd>Overflow processing.</dd>
 * <dt>{@link IndexManager}</dt>
 * <dd>Manages indices</dd>
 * <dt>{@link StoreManager}</dt>
 * <dd>Manages the journal and index segment files, including the release of
 * old resources.</dd>
 * <dt>{@link ResourceEvents}</dt>
 * <dd>Event reporting API</dd>
 * </dl>
 * 
 * @todo Transparent promotion of unpartitioned indices to indicate that support
 *       delete markers and can therefore undergo {@link #overflow()}? This is
 *       done by defining one partition that encompases the entire legal key
 *       range and setting the resource metadata for the view. However, I am not
 *       sure that the situation is likely to arise except if trying to import
 *       data from a {@link Journal} into an {@link IBigdataFederation}.
 *       <p>
 *       Transparent promotion of indices to support delete markers on
 *       {@link #overflow()}? We don't need to maintain delete markers until
 *       the first overflow event....
 *       <P>
 *       Do NOT break the ability to use concurrency control on unpartitioned
 *       indices -- note that overflow handling will only work on that support
 *       deletion markers.
 * 
 * @todo Document backup procedures for the journal (the journal is a
 *       log-structured store; it can be deployed on RAID for media robustness;
 *       can be safely copied using normal file copy mechanisms and restored;
 *       can be compacted offline; a compact snapshot of a commit point (such as
 *       the last commit point) can be generated online and used for recovery;
 *       and can be exported onto index segments which can later be restored to
 *       any journal, but those index segments need additional metadata to
 *       recreate the appropriate relations which is found in the sparse row
 *       store) and the federation (relies on service failover (primary and
 *       secondaries) and a history retention policy; can be recovered from
 *       existing index partitions in a bottom up matter, but that recovery code
 *       has not been written).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ResourceManager extends OverflowManager implements
        IPartitionIdFactory {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(ResourceManager.class);

    /**
     * Interface defines and documents the counters and counter namespaces for
     * the {@link ResourceManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IResourceManagerCounters
        extends IOverflowManagerCounters, IIndexManagerCounters, IStoreManagerCounters {
    
        /**
         * The namespace for counters pertaining to the {@link OverflowManager}.
         */
        String OverflowManager = "Overflow Manager";

        /**
         * The namespace for counters pertaining for overflow tasks (within the
         * {@link #OverflowManager} namespace).
         */
        String IndexPartitionTasks = "Overflow Tasks";
        
        /**
         * The namespace for counters pertaining to the {@link IndexManager}.
         */
        String IndexManager = "Index Manager";
        
        /** 
         * The namespace for counters pertaining to the {@link StoreManager}.
         */
        String StoreManager = "Store Manager";

        /**
         * The namespace for counters pertaining to the live
         * {@link ManagedJournal}.
         * <p>
         * Note: these counters are detached and reattached to the new live
         * journal during overflow processing.
         */
        String LiveJournal = "Live Journal";
        
    }

    /**
     * <strong>WARNING: The {@link DataService} transfers all of the children
     * from this object into the hierarchy reported by
     * {@link AbstractFederation#getServiceCounterSet()} and this object will be
     * empty thereafter.</strong>
     */
    synchronized public CounterSet getCounters() {
        
        if (root == null) {

            root = new CounterSet();

            // ResourceManager
            {

                // ... nothing really - its all under other headings.

            }
            
            // Live Journal
            {
                
                /*
                 * Note: these counters are detached and reattached to the new
                 * live journal during overflow processing.
                 * 
                 * @todo This assumes that the StoreManager is running.
                 * Normally, this will be true since the DataService does not
                 * setup its counter set until the store manager is running and
                 * the service UUID has been assigned. However, eagerly
                 * requesting the counters set would violate that assumption and
                 * cause an exception to be thrown here since the live journal
                 * is not defined until the StoreManager is running.
                 * 
                 * It would be best to modify this to attach the live journal
                 * counters when the StoreManager startup completes successfully
                 * rather than assuming that it already has done so. However,
                 * the counter set for the ResourceManager is not currently
                 * defined until the StoreManager is running...
                 */

                root.makePath(IResourceManagerCounters.LiveJournal).attach(
                        getLiveJournal().getCounters());

            }
            
            // OverflowManager
            {

                final CounterSet tmp = root
                        .makePath(IResourceManagerCounters.OverflowManager);

                tmp.addCounter(IOverflowManagerCounters.OverflowEnabled,
                        new Instrument<Boolean>() {
                            public void sample() {
                                setValue(isOverflowEnabled());
                            }
                        });

                tmp.addCounter(IOverflowManagerCounters.OverflowAllowed,
                        new Instrument<Boolean>() {
                            public void sample() {
                                setValue(isOverflowAllowed());
                            }
                        });

                tmp.addCounter(IOverflowManagerCounters.ShouldOverflow,
                        new Instrument<Boolean>() {
                            public void sample() {
                                setValue(shouldOverflow());
                            }
                        });

                tmp.addCounter(IOverflowManagerCounters.SynchronousOverflowCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(getSynchronousOverflowCount());
                            }
                        });

                tmp.addCounter(IOverflowManagerCounters.AsynchronousOverflowCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(getAsynchronousOverflowCount());
                            }
                        });

                tmp
                        .addCounter(
                                IOverflowManagerCounters.AsynchronousOverflowFailedCount,
                                new Instrument<Long>() {
                                    public void sample() {
                                        setValue(asyncOverflowFailedCounter
                                                .get());
                                    }
                                });

                tmp
                        .addCounter(
                                IOverflowManagerCounters.AsynchronousOverflowTaskFailedCount,
                                new Instrument<Long>() {
                                    public void sample() {
                                        setValue(asyncOverflowTaskFailedCounter
                                                .get());
                                    }
                                });

                tmp
                        .addCounter(
                                IOverflowManagerCounters.AsynchronousOverflowTaskCancelledCount,
                                new Instrument<Long>() {
                                    public void sample() {
                                        setValue(asyncOverflowTaskCancelledCounter
                                                .get());
                                    }
                                });

                {
                
                    final CounterSet tmp2 = tmp
                            .makePath(IResourceManagerCounters.IndexPartitionTasks);

                    tmp2.addCounter(IIndexPartitionTaskCounters.BuildCount,
                            new Instrument<Long>() {
                                public void sample() {
                                    setValue(indexPartitionBuildCounter.get());
                                }
                            });

                    tmp2.addCounter(IIndexPartitionTaskCounters.MergeCount,
                            new Instrument<Long>() {
                                public void sample() {
                                    setValue(indexPartitionMergeCounter.get());
                                }
                            });

                    tmp2.addCounter(IIndexPartitionTaskCounters.SplitCount,
                            new Instrument<Long>() {
                                public void sample() {
                                    setValue(indexPartitionSplitCounter.get());
                                }
                            });

                    tmp2.addCounter(IIndexPartitionTaskCounters.TailSplitCount,
                            new Instrument<Long>() {
                                public void sample() {
                                    setValue(indexPartitionTailSplitCounter.get());
                                }
                            });

                    tmp2.addCounter(IIndexPartitionTaskCounters.JoinCount,
                            new Instrument<Long>() {
                                public void sample() {
                                    setValue(indexPartitionJoinCounter.get());
                                }
                            });

                    tmp2.addCounter(IIndexPartitionTaskCounters.MoveCount,
                            new Instrument<Long>() {
                                public void sample() {
                                    setValue(indexPartitionMoveCounter.get());
                                }
                            });

                    tmp2.addCounter(IIndexPartitionTaskCounters.ReceiveCount,
                            new Instrument<Long>() {
                                public void sample() {
                                    setValue(indexPartitionReceiveCounter.get());
                                }
                            });

                }

            }

            // IndexManager
            {
                
                final CounterSet tmp = root
                        .makePath(IResourceManagerCounters.IndexManager);

                // save a reference.
                indexManagerRoot = tmp;
                
                tmp.addCounter(IIndexManagerCounters.StaleLocatorCacheCapacity,
                        new Instrument<Integer>() {
                            public void sample() {
                                setValue(staleLocatorCache.capacity());
                            }
                        });
                
                tmp.addCounter(IIndexManagerCounters.StaleLocatorCacheSize,
                        new Instrument<Integer>() {
                            public void sample() {
                                setValue(getStaleLocatorCount());
                            }
                        });
                
                if(true)
                    tmp.addCounter(IIndexManagerCounters.StaleLocators,
                        new Instrument<String>() {
                        public void sample() {
                            final StringBuilder sb = new StringBuilder();
                            final Iterator<ICacheEntry<String/* name */, StaleLocatorReason>> itr = staleLocatorCache
                                    .entryIterator();
                            while (itr.hasNext()) {
                                try {
                                    final ICacheEntry<String/* name */, StaleLocatorReason> entry = itr
                                            .next();
                                    sb.append(entry.getKey() + "="
                                            + entry.getObject() + "\n");
                                } catch (NoSuchElementException ex) {
                                    // Ignore - concurrent modification.
                                }
                            }
                            setValue(sb.toString());
                        }
                    });
                
                tmp.addCounter(IIndexManagerCounters.IndexCacheCapacity,
                        new Instrument<Integer>() {
                            public void sample() {
                                setValue(getIndexCacheCapacity());
                            }
                        });
                
                tmp.addCounter(IIndexManagerCounters.IndexCacheSize,
                        new Instrument<Integer>() {
                            public void sample() {
                                setValue(getIndexCacheSize());
                            }
                        });
                
                tmp.addCounter(IIndexManagerCounters.IndexSegmentCacheCapacity,
                        new Instrument<Integer>() {
                            public void sample() {
                                setValue(getIndexSegmentCacheCapacity());
                            }
                        });
                
                tmp.addCounter(IIndexManagerCounters.IndexSegmentCacheSize,
                        new Instrument<Integer>() {
                            public void sample() {
                                setValue(getIndexSegmentCacheSize());
                            }
                        });

                tmp.addCounter(IIndexManagerCounters.IndexSegmentOpenLeafCount,
                        new Instrument<Integer>() {
                            public void sample() {
                                setValue(getIndexSegmentOpenLeafCount());
                            }
                        });

                tmp.addCounter(IIndexManagerCounters.IndexSegmentOpenLeafByteCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(getIndexSegmentOpenLeafByteCount());
                            }
                        });

            }

            // StoreManager
            {

                final CounterSet tmp = root
                        .makePath(IResourceManagerCounters.StoreManager);

                tmp.addCounter(IStoreManagerCounters.DataDir,
                        new Instrument<String>() {
                            public void sample() {
                                setValue(dataDir == null ? "N/A" : dataDir
                                        .getAbsolutePath());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.TmpDir,
                        new Instrument<String>() {
                            public void sample() {
                                setValue(tmpDir == null ? "N/A" : tmpDir
                                        .getAbsolutePath());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.IsOpen,
                        new Instrument<Boolean>() {
                            public void sample() {
                                setValue(isOpen());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.IsStarting,
                        new Instrument<Boolean>() {
                            public void sample() {
                                setValue(isStarting());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.IsRunning,
                        new Instrument<Boolean>() {
                            public void sample() {
                                setValue(isRunning());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.StoreCacheCapacity,
                        new OneShotInstrument<Integer>(storeCache.capacity()));

                tmp.addCounter(IStoreManagerCounters.StoreCacheSize,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue((long) getStoreCacheSize());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.ManagedJournalCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue((long) getManagedJournalCount());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.ManagedSegmentStoreCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue((long) getManagedSegmentCount());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.JournalReopenCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(journalReopenCount.get());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.SegmentStoreReopenCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(segmentStoreReopenCount.get());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.JournalDeleteCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(journalDeleteCount.get());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.SegmentStoreDeleteCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(segmentStoreDeleteCount.get());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.BytesUnderManagement,
                        new Instrument<Long>() {
                            public void sample() {
                                if (isRunning()) {
                                    setValue(getBytesUnderManagement());
                                }
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.JournalBytesUnderManagement,
                        new Instrument<Long>() {
                            public void sample() {
                                if (isRunning()) {
                                    setValue(getJournalBytesUnderManagement());
                                }
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.SegmentBytesUnderManagement,
                        new Instrument<Long>() {
                            public void sample() {
                                if (isRunning()) {
                                    setValue(getSegmentBytesUnderManagement());
                                }
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.BytesDeleted,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(bytesDeleted.get());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.DataDirBytesAvailable,
                        new Instrument<Long>() {
                            public void sample() {
                                if (!isTransient())
                                    setValue(getDataDirFreeSpace());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.TmpDirBytesAvailable,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(getTempDirFreeSpace());
                            }
                        });

                tmp.addCounter(
                        IStoreManagerCounters.MaximumJournalSizeAtOverflow,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(maximumJournalSizeAtOverflow);
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.ReleaseTime,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(getReleaseTime());
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.LastOverflowTime,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(lastOverflowTime);
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.LastCommitTimePreserved,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(lastCommitTimePreserved);
                            }
                        });

                tmp.addCounter(IStoreManagerCounters.LastCommitTime,
                        new Instrument<Long>() {
                            public void sample() {
                                final ManagedJournal liveJournal;
                                try {
                                    liveJournal = getLiveJournal();
                                    setValue(liveJournal.getLastCommitTime());
                                } catch (Throwable t) {
                                    log.warn(t);
                                }
                            }
                        });

                /*
                 * Performance counters for the service used to let other data
                 * services read index segments or journals from this service.
                 */
                final CounterSet tmp2 = tmp.makePath("resourceService");

                tmp2.attach(resourceService.counters.getCounters());
                
            }

        }

        return root;

    }

    private CounterSet root;

    /**
     * The counter set that corresponds to the {@link IndexManager}.
     */
    public CounterSet getIndexManagerCounters() {

        if (indexManagerRoot == null) {
            
            getCounters();
            
        }
        
        return indexManagerRoot;
        
    }
    private CounterSet indexManagerRoot;
        
    /**
     * {@link ResourceManager} options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends OverflowManager.Options {
        
    }
    
    private IConcurrencyManager concurrencyManager;
    
    public IConcurrencyManager getConcurrencyManager() {

        if (concurrencyManager == null) {

            // Not assigned!

            throw new IllegalStateException();

        }

        return concurrencyManager;
        
    }

    public void setConcurrencyManager(
            final IConcurrencyManager concurrencyManager) {

        if (concurrencyManager == null)
            throw new IllegalArgumentException();

        if (this.concurrencyManager != null)
            throw new IllegalStateException();

        this.concurrencyManager = concurrencyManager;
        
    }

    /**
     * (Re-)open the {@link ResourceManager}.
     * <p>
     * Note: You MUST use {@link #setConcurrencyManager(IConcurrencyManager)}
     * after calling this constructor (the parameter can not be passed in since
     * there is a circular dependency between the {@link IConcurrencyManager}
     * and {@link ManagedJournal#getLocalTransactionManager()}.
     * 
     * @param properties
     *            See {@link Options}.
     * 
     * @see DataService#start()
     */
    public ResourceManager(Properties properties) {

        super(properties);
        
    }

    /**
     * Requests a new index partition identifier from the
     * {@link MetadataService} for the specified scale-out index (RMI).
     * 
     * @return The new index partition identifier.
     * 
     * @throws RuntimeException
     *             if something goes wrong.
     */
    public int nextPartitionId(final String scaleOutIndexName) {

        final IMetadataService mds = getFederation().getMetadataService();

        if (mds == null) {

            throw new RuntimeException("Metadata service not discovered.");
            
        }
        
        try {

            // obtain new partition identifier from the metadata service (RMI)
            final int newPartitionId = mds.nextPartitionId(scaleOutIndexName);

            return newPartitionId;

        } catch (Throwable t) {

            throw new RuntimeException(t);

        }

    }

}
