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

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
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

            // OverflowManager
            {

                final CounterSet tmp = root.makePath("Overflow Manager");

                tmp.addCounter("Overflow Count", new Instrument<Long>() {
                    public void sample() {
                        setValue(getOverflowCount());
                    }
                });

                tmp.addCounter("Overflow Allowed", new Instrument<Boolean>() {
                    public void sample() {
                        setValue(overflowAllowed.get());
                    }
                });
                
                tmp.addCounter("Should Overflow", new Instrument<Boolean>() {
                    public void sample() {
                        setValue(shouldOverflow());
                    }
                });
                
            }
            
            // IndexManager
            {
                
                final CounterSet tmp = root.makePath("Index Manager");

                tmp.addCounter("Stale Locator Cache Size",
                        new Instrument<Long>() {
                            public void sample() {
                                setValue((long) getStaleLocatorCount());
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

                tmp.addCounter("Journal Count", new Instrument<Long>(){
                    public void sample() {setValue((long)getJournalCount());}
                });

                tmp.addCounter("Index Segment Count", new Instrument<Long>(){
                    public void sample() {setValue((long)getIndexSegmentCount());}
                });

                tmp.addCounter("Minimum Release Age", new Instrument<Long>(){
                    public void sample() {setValue(minReleaseAge);}
                });

                tmp.addCounter("Release Time", new Instrument<Long>(){
                    public void sample() {setValue(releaseTime);}
                });

                tmp.addCounter("Effective Release Time", new Instrument<Long>(){
                    public void sample() {setValue(lastEffectiveReleaseTime);}
                });

            }
            
            /*
             * Note: these counters are detached and reattached to the new
             * live journal during overflow processing.
             */
            root.makePath("Live Journal").attach(getLiveJournal().getCounters());
                        
        }
        
        return root;
        
    }
    private CounterSet root;
        
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
