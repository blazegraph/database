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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.ICacheEntry;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.Journal;
import com.bigdata.service.IBigdataFederation;

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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ResourceManager extends OverflowManager implements IResourceManager {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(ResourceManager.class);

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * Return the {@link CounterSet}.
     */
    synchronized public CounterSet getCounters() {
        
        if (root == null) {

            root = new CounterSet();

            // ResourceManager
            {

                // ... nothing really - its all under other headings.
                
            }
            
            // OverflowManager
            {

                final CounterSet tmp = root.makePath("Overflow Manager");

                tmp.addCounter("Overflow Enabled", new Instrument<Boolean>() {
                    public void sample() {
                        setValue(isOverflowEnabled());
                    }
                });
                
                tmp.addCounter("Overflow Allowed", new Instrument<Boolean>() {
                    public void sample() {
                        setValue(isOverflowAllowed());
                    }
                });
                
                tmp.addCounter("Should Overflow", new Instrument<Boolean>() {
                    public void sample() {
                        setValue(shouldOverflow());
                    }
                });
                
                tmp.addCounter("Overflow Count", new Instrument<Long>() {
                    public void sample() {
                        setValue(getOverflowCount());
                    }
                });
                
                tmp.addCounter("Overflow Failed Count", new Instrument<Long>() {
                    public void sample() {
                        setValue(overflowFailedCounter.get());
                    }
                });

                tmp.addCounter("Index Partition Build Count", new Instrument<Long>() {
                    public void sample() {
                        setValue(buildCounter.get());
                    }
                });
                
                tmp.addCounter("Index Partition Split Count", new Instrument<Long>() {
                    public void sample() {
                        setValue(splitCounter.get());
                    }
                });

                tmp.addCounter("Index Partition Join Count", new Instrument<Long>() {
                    public void sample() {
                        setValue(joinCounter.get());
                    }
                });

                tmp.addCounter("Index Partition Move Count", new Instrument<Long>() {
                    public void sample() {
                        setValue(moveCounter.get());
                    }
                });

            }
            
            // IndexManager
            {
                
                final CounterSet tmp = root.makePath("Index Manager");

                // save a reference.
                indexManagerRoot = tmp;
                
                tmp.addCounter("Stale Locator Cache Size",
                        new Instrument<Long>() {
                            public void sample() {
                                setValue((long) getStaleLocatorCount());
                            }
                        });
                
                if(true)
                    tmp.addCounter("Stale Locators",
                        new Instrument<String>() {
                            public void sample() {
                                StringBuilder sb = new StringBuilder();
                                Iterator<ICacheEntry<String/*name*/,String/*reason*/>> itr = staleLocatorCache.entryIterator();
                                while(itr.hasNext()) {
                                    try {
                                    ICacheEntry<String/*name*/,String/*reason*/> entry = itr.next();
                                    sb.append(entry.getKey()+"="+entry.getObject()+"\n");
                                    } catch(NoSuchElementException ex) {
                                        // Ignore - concurrent modification.
                                    }
                                }
                                setValue(sb.toString());
                            }
                        });
                
                tmp.addCounter("Index Segment Cache Size",
                        new Instrument<Long>() {
                            public void sample() {
                                setValue((long) getIndexSegmentCacheSize());
                            }
                        });
                
            }

            // StoreManager
            {
                
                final CounterSet tmp = root.makePath("Store Manager");

                tmp.addCounter("DataDir", new Instrument<String>() {
                    public void sample() {
                        setValue(dataDir==null?"N/A":dataDir.getAbsolutePath());
                    }
                });

                tmp.addCounter("isOpen", new Instrument<Boolean>(){
                    public void sample() {setValue(isOpen());}
                });
                
                tmp.addCounter("isStarting", new Instrument<Boolean>(){
                    public void sample() {setValue(isStarting());}
                });

                tmp.addCounter("isRunning", new Instrument<Boolean>(){
                    public void sample() {setValue(isRunning());}
                });

                tmp.addCounter("Store Cache Capacity",
                        new OneShotInstrument<Integer>(storeCache.capacity()));

                tmp.addCounter("Store Cache Size", new Instrument<Long>(){
                    public void sample() {setValue((long)getStoreCacheSize());}
                });

                tmp.addCounter("Managed Journal Count", new Instrument<Long>(){
                    public void sample() {setValue((long)getManagedJournalCount());}
                });

                tmp.addCounter("Managed Segment Store Count", new Instrument<Long>(){
                    public void sample() {setValue((long)getManagedIndexSegmentCount());}
                });

                tmp.addCounter("Journal (Re-)open Count", new Instrument<Long>(){
                    public void sample() {setValue(journalReopenCount.get());}
                });

                tmp.addCounter("Segment Store (Re-)open Count", new Instrument<Long>(){
                    public void sample() {setValue(segmentStoreReopenCount.get());}
                });

                tmp.addCounter("Journal Delete Count", new Instrument<Long>(){
                    public void sample() {setValue(journalDeleteCount.get());}
                });

                tmp.addCounter("Segment Store Delete Count", new Instrument<Long>(){
                    public void sample() {setValue(segmentStoreDeleteCount.get());}
                });

                tmp.addCounter("Bytes Under Management", new Instrument<Long>(){
                    public void sample() {
                                if (isRunning()) {
                                    setValue(getBytesUnderManagement());
                                }
                            }
                        });
                
                tmp.addCounter("Bytes Deleted", new Instrument<Long>(){
                    public void sample() {
                        setValue(bytesDeleted.get());
                    }
                });

                tmp.addCounter("Free Space on Data Volume", new Instrument<Long>(){
                    public void sample() {
                        setValue(getDataDirFreeSpace());
                    }
                });

                tmp.addCounter("Free Space on Temp Volume", new Instrument<Long>(){
                    public void sample() {
                        setValue(getTempDirFreeSpace());
                    }
                });

                tmp.addCounter("Minimum Release Age",
                        new OneShotInstrument<Long>(minReleaseAge));

                tmp.addCounter("Release Time", new Instrument<Long>(){
                    public void sample() {setValue(releaseTime);}
                });

                tmp.addCounter("Last Commit Time Preserved", new Instrument<Long>(){
                    public void sample() {setValue(lastCommitTimePreserved);}
                });

            }
            
            /*
             * Note: these counters are detached and reattached to the new live
             * journal during overflow processing.
             * 
             * @todo This assumes that the StoreManager is running. Normally,
             * this will be true since the DataService does not setup its
             * counter set until the store manager is running and the service
             * UUID has been assigned. However, eagerly requesting the counters
             * set would violate that assumption and cause an exception to be
             * thrown here since the live journal is not defined until the
             * StoreManager is running.
             * 
             * It would be best to modify this to attach the live journal
             * counters when the StoreManager startup completes successfully
             * rather than assuming that it already has done so. However, the
             * counter set for the ResourceManager is not currently defined
             * until the StoreManager is running...
             */
            root.makePath("Live Journal").attach(getLiveJournal().getCounters());
                        
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
        
        if(concurrencyManager==null) {
            
            // Not assigned!
            
            throw new IllegalStateException();
            
        }
        
        return concurrencyManager;
        
    }

    public void setConcurrencyManager(IConcurrencyManager concurrencyManager) {

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
     * and {@link #commit(long)} on this class, which requires access to the
     * {@link IConcurrencyManager} to submit a task).
     * 
     * @param properties
     *            See {@link Options}.
     * 
     * @see #start()
     */
    public ResourceManager(Properties properties) {

        super(properties);
        
    }

}
