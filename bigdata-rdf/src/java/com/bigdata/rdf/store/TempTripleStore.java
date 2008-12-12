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
 * Created on Jan 3, 2007
 */

package com.bigdata.rdf.store;

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.service.IBigdataFederation;

/**
 * A temporary triple store based on the <em>bigdata</em> architecture. Data
 * is buffered in memory but will overflow to disk for large stores. The backing
 * store is a {@link TemporaryStore}.
 * <p>
 * Note: the {@link TempTripleStore} declares indices that do NOT support
 * isolation. This offers a significant performance boost when you do not need
 * transactions or the ability to purge historical data versions from the store
 * as they age.
 * <p>
 * Note: Users of the {@link TempTripleStore} may find it worthwhile to turn off
 * a variety of options in order to minimize the time and space burden of the
 * temporary store depending on the use which will be made of it, including
 * {@link Options#LEXICON} and {@link Options#ONE_ACCESS_PATH}.
 * <p>
 * Note: Multiple KBs MAY be created in the backing {@link TemporaryStore} but
 * all MUST be assigned the {@link TemporaryStore#getUUID()} as their prefix in
 * order to avoid possible conflicts within a global namespace. This is
 * especially important when the relations in the {@link TemporaryStore} are
 * resolvable by an {@link IBigdataFederation} or {@link Journal}.
 * <p>
 * Note: This class is often used to support inference. When so used, the
 * statement indices are populated with the term identifiers from the main
 * database and the {@link SPORelation} in the {@link TempTripleStore} is
 * disabled using {@link Options#LEXICON}.
 * <p>
 * Note: If you want an in-memory {@link ITripleStore} that supports commit and
 * abort then use a {@link LocalTripleStore} and specify
 * {@link com.bigdata.journal.Options#BUFFER_MODE} as
 * {@link BufferMode#Temporary} or as {@link BufferMode#Transient} if you want
 * the triple store to begin in memory and overflow to disk if necessary. Both
 * of these configurations provide full concurrency control and group commit.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TempTripleStore extends AbstractLocalTripleStore {
    
    private final TemporaryStore store;
    
    public TemporaryStore getIndexManager() {
        
        return store;
        
    }

    /**
     * NOP.
     * <p>
     * Note: since multiple {@link TempTripleStore}s may be created on the same
     * backing {@link TemporaryStore} it is NOT safe to perform a
     * {@link TemporaryStore#checkpoint()}. There is no coordination of write
     * access to the indices so the checkpoint can not be atomic. Therefore this
     * method is a NOP and {@link #abort()} will throw an exception.
     */
    final public void commit() {
     
//        final long begin = System.currentTimeMillis();

        super.commit();
        
//        final long checkpointAddr = getIndexManager().checkpoint();
//
//        final long elapsed = System.currentTimeMillis() - begin;
//
//        if (log.isInfoEnabled())
//            log.info("latency=" + elapsed + "ms, checkpointAddr="
//                    + checkpointAddr);

    }

    /**
     * Not supported.
     * 
     * @throws UnsupportedOperationException
     */
    final public void abort() {

        throw new UnsupportedOperationException();
        
//        super.abort();
//        
//        // discard the write sets.
//        getIndexManager().restoreLastCheckpoint();

    }
    
    final public boolean isStable() {
        
        return store.isStable();
        
    }
    
    /**
     * Causes the {@link TempTripleStore} to be {@link #destroy()}ed, but does
     * not reclaim space in the backing {@link TemporaryStore} and does not
     * close the backing {@link TemporaryStore}.
     */
    final public void close() {
        
//        store.close();

        destroy();

        super.close();
        
    }

    /**
     * Deletes the backing {@link TemporaryStore}, thereby destroying all
     * {@link TempTripleStore}s on that {@link TemporaryStore}. After calling
     * this method you will see an {@link IllegalStateException} if you attempt
     * further operations on {@link TempTripleStore}s that were backed by the
     * backing {@link TemporaryStore}.
     */
    final public void closeAndDelete() {
        
        store.destroy();
        
        super.closeAndDelete();
        
    }
    
    /**
     * 
     * @todo define options for {@link TemporaryStore} and then extend them
     *       here.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends AbstractTripleStore.Options { //, TemporaryStore.Options {
        
    }
    
    /**
     * Create a transient {@link ITripleStore} backed by a new
     * {@link TemporaryStore}.
     * 
     * @param properties
     *            See {@link Options}.
     */
    public TempTripleStore(Properties properties) {
       
        this(properties, null);
        
    }
    
    /**
     * Create a transient {@link ITripleStore} backed by a new
     * {@link TemporaryStore}. The {@link ITripleStore} will default its
     * properties based on those specified for the optional <i>db</i> and then
     * override those defaults using the given <i>properties</i>.
     * <p>
     * Note: This variant is especially useful when the {@link TempTripleStore}
     * will have its own lexicon and you need it to be compatible with the
     * lexicon for the <i>db</i>.
     * <p>
     * Note: When <i>db</i> is non-<code>null</code>, the relations on the
     * {@link TempTripleStore} will be locatable by the specified <i>db</i>.
     * 
     * @param properties
     *            Overrides for the database's properties.
     * @param db
     *            The optional database (a) will establish the defaults for the
     *            {@link TempTripleStore}; and (b) will be able to locate
     *            relations declared on the backing {@link TemporaryStore}.
     * 
     * @deprecated Use
     *             {@link #TempTripleStore(TemporaryStore, Properties, AbstractTripleStore)}
     *             instead and provide the {@link TemporaryStore} reference
     *             returned by {@link IIndexStore#getTempStore()}. This has the
     *             advantage of reusing a single shared {@link TemporaryStore}
     *             instance until it becomes "large" and then allocating a new
     *             instance (note that each instance will consume a direct
     *             {@link ByteBuffer} from the {@link DirectBufferPool}). This
     *             is especially important for operations like
     *             {@link TruthMaintenance} which have to create a lot of
     *             temporary stores.
     */
    public TempTripleStore(Properties properties, AbstractTripleStore db) {
        
        this(new TemporaryStore(), properties, db);

    }

    /**
     * Variant for creating a(nother) {@link TempTripleStore} on the same
     * {@link TemporaryStore}. The {@link TempTripleStore} will have its own
     * namespace.
     * 
     * @param store
     *            The {@link TemporaryStore}.
     * @param properties
     *            Overrides for the database's properties.
     * @param db
     *            The optional database (a) will establish the defaults for the
     *            {@link TempTripleStore}; and (b) will be able to locate
     *            relations declared on the backing {@link TemporaryStore}.
     */
    public TempTripleStore(final TemporaryStore store,
            final Properties properties, final AbstractTripleStore db) {

        this(store, db == null ? properties : stackProperties(properties, db));

        if (INFO) {

            log.info("new temporary store: " + store.getFile() + ", namespace="
                    + getNamespace());

        }

        if (db != null) {

            ((DefaultResourceLocator) db.getIndexManager().getResourceLocator())
                    .add(store);

        }

        /*
         * Create the KB for this ctor variant!
         */

        create();

    }

    /**
     * Note: This is here just to make it easy to have the reference to the
     * [store] and its [uuid] when we create one in the calling ctor.
     */
    private TempTripleStore(TemporaryStore store, Properties properties) {

        this(store, UUID.randomUUID() + "kb", ITx.UNISOLATED, properties);
        
    }

    /**
     * Ctor specified by {@link DefaultResourceLocator}.
     * 
     * @param indexManager
     * @param namespace
     * @param timestamp
     * @param properties
     */
    public TempTripleStore(IIndexManager indexManager, String namespace,
            Long timestamp, Properties properties) {

        super(indexManager, namespace, timestamp, properties);

        store = (TemporaryStore) indexManager;

        if(log.isInfoEnabled()) {
            
            log.info("view on existing temporary store: "+store.getUUID());
            
        }
        
    }
    
    /**
     * Stacks the <i>properties</i> on top of the <i>db</i>'s properties so
     * that the databases properties will be treated as defaults and anything in
     * <i>properties</i> will override anything in database's properties.
     * 
     * @param properties
     *            The properties for the {@link TempTripleStore}.
     * @param db
     *            The database from which we will obtain default properties.
     * 
     * @return The stacked properties.
     */
    private static Properties stackProperties(Properties properties, AbstractTripleStore db) {
        
        Properties tmp = db.getProperties();
        
        Enumeration e = properties.keys();
        
        while(e.hasMoreElements()) {
            
            Object ekey = e.nextElement();
            
            if(!(ekey instanceof String)) {
                
                continue;
                
            }
            
            final String key = (String)ekey;
            
            tmp.setProperty(key,properties.getProperty(key));
            
        }
        
        return tmp;
        
    }

    /**
     * This store is NOT safe for concurrent operations on an
     * {@link ITx#UNISOLATED} index. However, it does support concurrent readers
     * on the {@link ITx#READ_COMMITTED} view of an index.
     */
    public boolean isConcurrent() {

        return false;
        
    }
    
}
