/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Mar 20, 2009
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.CommitRecordIndex.Entry;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.accesspath.TupleObjectResolver;

import cutthecrap.utils.striterators.Striterator;

/**
 * Class capable of receiving {@link Event}s from remote services. Start events
 * are maintained in a cache until their corresponding end event is received at
 * which point they are propagated to an {@link EventBTree} which is used for
 * reporting purposes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EventReceiver implements IEventReceivingService,
        IEventReportingService {

    /**
     * Logs {@link Event}s in a tab-delimited format @ INFO.
     * <p>
     * Note: If you enable more detailed logging (DEBUG, etc). then that will
     * get mixed up with your tab-delimited events log and you may have to
     * filter it out before you can process the events log analytically.
     */
    protected static transient final Logger log = Logger
            .getLogger(EventReceiver.class);
    
    /**
     * The maximum age of an {@link Event} that will be keep "on the books".
     * Events older than this are purged.
     */
    protected final long eventHistoryMillis;
    
    /**
     * Basically a ring buffer of events without a capacity limit and with
     * random access by the event {@link UUID}. New events are added to the
     * collection by {@link #notifyEvent(Event)}. That method also scans the
     * head of the collection, purging any events that are older than the
     * desired #of minutes of event history to retain.
     * <p>
     * Since this is a "linked" collection, the order of the events in the
     * collection will always reflect their arrival time. This lets us scan the
     * events in temporal order, filtering for desired criteria and makes it
     * possible to prune the events in the buffer as new events arrive.
     */
    protected final LinkedHashMap<UUID,Event> eventCache;

    /**
     * The completed events are removed from the {@link #eventCache} and written
     * onto the {@link #eventBTree}. This is done in order to get the events
     * out of memory and onto disk and to decouple the
     * {@link IEventReceivingService} from the {@link IEventReportingService}.
     */
    protected final UnisolatedReadWriteIndex ndx;
    
    /**
     * The {@link ITupleSerializer} reference is cached.
     */
    private final ITupleSerializer<Long, Event> tupleSer;
    
    /**
     * 
     * @param eventHistoryMillis
     *            The maximum age of an {@link Event} that will be keep "on the
     *            books". Events older than this are purged. An error is logged
     *            if an event is purged before its end() event arrives. This
     *            generally indicates a code path where {@link Event#end()} is
     *            not getting called but could also indicate a disconnected
     *            client or service.
     * @param eventStore
     *            A {@link BTree} used to record the completed events and to
     *            implement the {@link IEventReportingService} interface.
     */
    @SuppressWarnings("unchecked")
    public EventReceiver(final long eventHistoryMillis,
            final EventBTree eventBTree) {

        if (eventHistoryMillis <= 0)
            throw new IllegalArgumentException();

        if (eventBTree == null)
            throw new IllegalArgumentException();
        
        this.eventHistoryMillis = eventHistoryMillis;

        /*
         * Note: the initial capacity is not so critical here as this only
         * stores the events whose matching end event we are awaiting.
         */

        this.eventCache = new LinkedHashMap<UUID, Event>(1000/* initialCapacity */);

        this.ndx = new UnisolatedReadWriteIndex(eventBTree);
        
        this.tupleSer = eventBTree.getIndexMetadata().getTupleSerializer();
        
    }

    /**
     * Acquire and return the write lock for the {@link EventBTree}. The caller
     * MUST {@link Lock#unlock()} the lock!
     */
    public Lock getWriteLock() {
        
        return ndx.writeLock();
        
    }
    
    /**
     * A {@link BTree} whose keys are event start times and whose values are the
     * serialized {@link Event}s. 
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class EventBTree extends BTree {

        /**
         * @param store
         * @param checkpoint
         * @param metadata
         */
        public EventBTree(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata) {

            super(store, checkpoint, metadata);
            
        }
        
        /**
         * Create a new instance.
         * 
         * @param store
         *            The backing store.
         * 
         * @return The new instance.
         */
        static public EventBTree create(final IRawStore store) {
        
            final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBTreeClassName(EventBTree.class.getName());
            
            metadata.setTupleSerializer(new EventBTreeTupleSerializer(
                    new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG)));
            
            return (EventBTree) BTree.create(store, metadata);
            
        }

        static public EventBTree createTransient() {
            
            final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
            
            metadata.setBTreeClassName(EventBTree.class.getName());
            
            metadata.setTupleSerializer(new EventBTreeTupleSerializer(
                    new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG)));
            
            return (EventBTree) BTree.createTransient(metadata);
            
        }

        /**
         * Encapsulates key and value formation for the {@link EventBTree}.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        static protected class EventBTreeTupleSerializer extends
                DefaultTupleSerializer<Long, Entry> {

            /**
             * 
             */
            private static final long serialVersionUID = -8429751113713375293L;

            /**
             * De-serialization ctor.
             */
            public EventBTreeTupleSerializer() {

                super();
                
            }

            /**
             * Ctor when creating a new instance.
             * 
             * @param keyBuilderFactory
             */
            public EventBTreeTupleSerializer(
                    final IKeyBuilderFactory keyBuilderFactory) {
                
                super(keyBuilderFactory);

            }
            
            /**
             * Decodes the key as a timestamp.
             */
            @Override
            public Long deserializeKey(ITuple tuple) {

                final byte[] key = tuple.getKeyBuffer().array();

                final long id = KeyBuilder.decodeLong(key, 0);

                return id;

            }

            /**
             * Return the unsigned byte[] key for a timestamp.
             * 
             * @param obj
             *            A timestamp.
             */
            @Override
            public byte[] serializeKey(Object obj) {

                return getKeyBuilder().reset().append((Long) obj).getKey();

            }

        }

    }

    /**
     * Accepts the event, either updates the existing event with the same
     * {@link UUID} or adds the event to the set of recent events, and then
     * prunes the set of recent events so that all completed events older than
     * {@link #eventHistoryMillis} are discarded.
     * <p>
     * An error is logged if an event is purged before its end() event arrives.
     * This generally indicates a code path where {@link Event#end()} is not
     * getting called but could also indicate a disconnected client or service.
     */
    public void notifyEvent(final Event e) throws IOException {

        if (e == null)
            throw new IllegalArgumentException();
        
        // timestamp for start/end time of the event.
        final long now = System.currentTimeMillis();
        
        synchronized (eventCache) {

            pruneHistory(now);

            /*
             * Test the cache.
             */
            final Event t = eventCache.get(e.eventUUID);

            if (t == null ) {

                /*
                 * Add to the cache (prevents retriggering if end() was called
                 * before the start() event was sent by the client.
                 */
                eventCache.put(e.eventUUID, e);

                // timestamp when we got this event.
                e.receiptTime = now;

                if(e.isComplete()) {

                    // log the completed event.
                    logEvent(e);

                }                

            } else {

                /*
                 * There is a event in the cache which has not yet been flagged
                 * as [complete]. We update the event's endTime and the details
                 * with from the new event and flag it as [complete], log it,
                 * and record it on the B+Tree.
                 * 
                 * Note: We do not immediately remove [complete] events from the
                 * [eventCache] for the same reason. If the client does
                 * e.start() followed by e.end() before the end event is sent
                 * then the start event will have the endTime set. This happens
                 * because the same event instance is being updated by the end()
                 * event.
                 */

                if (!t.isComplete()) {

                    // copy potentially updated details from the new event.
                    t.details = e.details;

                    // grab the end time from the new event.
                    t.endTime = e.endTime;

                    // mark the event as complete.
                    t.complete = true;

                    // log the completed event.
                    logEvent(t);

                }

            }

        }

    }
    
    /**
     * Any completed events which are older (LT) than the cutoff point (now -
     * {@link #eventHistoryMillis}} are discarded.
     */
    protected void pruneHistory(final long now) {
       
        final long cutoff = now - eventHistoryMillis;

        final Iterator<Event> itr = eventCache.values().iterator();

        int npruned = 0;
        
        while (itr.hasNext()) {

            final Event t = itr.next();

            if (t.receiptTime > cutoff) {

                break;

            }

            // discard old event.
            itr.remove();

            if (!t.isComplete()) {

                /*
                 * This presumes that events should complete within the
                 * event history retention period. A failure to receive the
                 * end() event most likely indicates that the client has a
                 * code path where end() is not invoked for the event.
                 */
                
                log.error("No end? " + t);

            }

            npruned++;

        }

        if (log.isDebugEnabled())
            log.debug("There are " + eventCache.size() + " events : cutoff="
                    + cutoff + ", #pruned " + npruned);

    }

    /**
     * Logs the completed event using a tab-delimited format @ INFO on
     * {@link #log}. Generally, {@link #log} should be directed to a file for
     * post-mortem analysis of the events, e.g., by importing the file into a
     * worksheet, using a pivot table, etc.
     * 
     * @todo if this event proves to be a bottleneck then a queue could be used
     *       a thread could migrate events from the queue to the log and the
     *       BTree.
     */
    protected void logEvent(final Event e) {

        if (e == null)
            throw new IllegalArgumentException();

        if (!e.isComplete())
            throw new IllegalArgumentException();

        if (log.isInfoEnabled()) {

            log.info(e.toString());

        }

        ndx.insert(e.startTime, e);
        
    }

    public long rangeCount(final long fromTime, final long toTime) {

        return ndx.rangeCount(tupleSer.serializeKey(fromTime),
                tupleSer.serializeKey(toTime));

    }

    @SuppressWarnings("unchecked")
    public Iterator<Event> rangeIterator(final long fromTime, final long toTime) {

        final ITupleIterator<Event> tupleItr = ndx.rangeIterator(tupleSer
                .serializeKey(fromTime), tupleSer.serializeKey(toTime));

        final Iterator<Event> itr = new Striterator(tupleItr)
                .addFilter(new TupleObjectResolver());

        return itr;
        
    }
    
}
