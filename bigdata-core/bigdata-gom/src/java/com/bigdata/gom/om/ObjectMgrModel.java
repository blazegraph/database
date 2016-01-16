/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Mar 19, 2012
 */
package com.bigdata.gom.om;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Base class for {@link IObjectManager} implementations. This class handles
 * {@link IObjectManager} protocol for maintaining an transaction edit list.
 * Concrete implementations need to provide for communication with the database
 * (either remote or embedded) and the DESCRIBE (aka Object) cache.
 * 
 * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn
 *         Cutcher</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class ObjectMgrModel implements IObjectManager {

    private static final Logger log = Logger.getLogger(ObjectMgrModel.class);

    /**
     * The {@link UUID} for this object manager instance.
     */
    private final UUID m_uuid;
    
    protected final BigdataValueFactory m_valueFactory;
    
    /** Object Creation and ID Management patterns. */
    private final IIDGenerator m_idGenerator;

    /**
     * The "running object table." Dirty objects are wired into this table by
     * the existence of a hard reference in the {@link #m_dirtyGPOs} list. The
     * keys are either {@link Resource}s or {@link Statement}s.
     */
    private final ConcurrentWeakValueCache<Object, IGPO> m_dict;
	
    /**
     * This is only for the predicates and provides the guarantee that we can
     * reference test on predicates within the scope of a given object manager.
     */
    private final ConcurrentHashMap<BigdataURI, BigdataURI> m_internedKeys = new ConcurrentHashMap<BigdataURI, BigdataURI>();

    /**
     * We need to maintain a dirty list in order to pin object references that
     * are dirty. On commit, we need to send the retracts and the asserts in a
     * single operation. The GPO.GPOEntry tracks those individual asserts and
     * retracts.
     */
    // private final List<GPO> m_dirtyGPOs = new LinkedList<GPO>();
    // Sample code indicates that an ArrayList is less overhead than a LinkedList
    private final List<GPO> m_dirtyGPOs = new ArrayList<GPO>();

	private final URI s_nmeMgr;

    /**
     * A lock for things which need to be serialized, initially just the native
     * transaction stuff. Avoid using "synchronized(this)" or the synchronized
     * keyword as that forces everything to contend for the same lock. If you
     * can use different locks for different types of things then you have
     * better concurrency (but, of course, only as appropriate).
     */
    private final Lock lock = new ReentrantLock();
    
    /**
     * Default to maximum dirty list size to lock out any incremental flushing.
     * <p>
     * Note: Incremental eviction breaks the ACID contract for updates. Thus,
     * the dirty list should not be limited in capacity.
     */
    protected int m_maxDirtyListSize = Integer.MAX_VALUE;
	
    /**
     * The native transaction counter.
     */
	private int m_transactionCounter = 0;
	
    /**
     * 
     * @param endpoint
     *            The SPARQL endpoint that can be used to communicate with the
     *            database.
     * @param valueFactory
     *            The value factory.
     */
    public ObjectMgrModel(
            final String endpoint, 
            final BigdataValueFactory valueFactory) {

		m_valueFactory = valueFactory;
		
		m_uuid = UUID.randomUUID();

		m_idGenerator = new IDGenerator(endpoint, m_uuid, m_valueFactory);
        
        /*
         * FIXME UUIG COINING. Plus this needs to be global if we have a
         * "name manager" object. Frankly, I do not see any reason to have
         * "named roots" in RDF GOM. Any URI can be a named root - you just need
         * to use the URI!
         */
		s_nmeMgr = m_valueFactory.createURI("gpo:nmeMgr/"+m_uuid);

		/*
		 * Note: This sets the hard reference queue capacity.
		 */
        m_dict = new ConcurrentWeakValueCache<Object, IGPO>(1000/* queueCapacity */);

	}
	
	public IGPO getDefaultNameMgr() {
		
	    return getGPO(s_nmeMgr);
	    
	}
	
	public UUID getID() {
		
	    return m_uuid;
	    
	}
	
    @Override
    final public BigdataValueFactory getValueFactory() {

        return m_valueFactory;

    }

    /**
     * Intern a predicate (internal API). This provides the guarantee that we
     * can use reference tests (<code>==</code>) for URIs within the scope of a
     * given object manager.
     * 
     * @param key
     *            The predicate.
     * 
     * @return The interned version of the predicate.
     */
    public BigdataURI internKey(final URI aKey) {

        // Ensure URI is for the namespace associated with this OM.
        final BigdataURI key = m_valueFactory.asValue(aKey);
        
        // Internal the URI.
        final BigdataURI old = m_internedKeys.putIfAbsent(key, key);

        // Resolve data race.
        final BigdataURI uri = old != null ? old : key;

		return uri;

    }

    /**
     * Make a best effort attempt to use the {@link Resource} associated with an
     * {@link IGPO} in the running object table
     * 
     * @param t
     *            Some identifier.
     *            
     * @return Either the same reference or one that will be canonical as long
     *         as that {@link IGPO} remains pinned in the running object table.
     */
    public <T> T bestEffortIntern(final T t) {

        if (t instanceof Resource) {

            final IGPO gpo = m_dict.get(t);

            if (gpo == null)
                return t;

            return (T) gpo.getId();

        }

        return t;
        
    }

    /**
     * GPOs are added to the dirty list when initially modified. The dirty list
     * is not bounded. Large updates should be done using the RDF and SPARQL
     * layer which do not have this implicit scaling limit.
     * <p>
     * Note: We can not do incremental eviction unless we are holding open a
     * connection to the database that will isolate those edits. This could be
     * done in principle with either full read/write transactions or with the
     * unisolated connection (if embedded) but we do not yet support remove
     * create/run/commit for full read/write transactions in the NSS REST API.
     * The problem with holding the unisolated connection across incremental
     * updates is that it will lock out any other updates against the backing
     * store for the life cycle of the object manager.
     */
	public void addToDirtyList(final GPO gpo) {

	    if(gpo == null)
	        throw new IllegalArgumentException();

        if (!gpo.isDirty())
            throw new IllegalStateException();

	    m_dirtyGPOs.add(gpo);
	    
		if (m_dirtyGPOs.size() > m_maxDirtyListSize) {

		    if (log.isTraceEnabled())
				log.trace("Incremental flush of dirty objects");
			
			flushDirtyObjects();

		}

	}
	
	/**
	 * 
	 * @return size of dirty list
	 */
	public int getDirtyObjectCount() {
		return m_dirtyGPOs.size();
	}
	
//	abstract void flushTerms();

	/**
	 * Commit.
	 */
	private void flushDirtyObjects() {

//	    // prepare values
//		final Iterator<GPO> newValues = m_dirtyGPOs.iterator();
//		while (newValues.hasNext()) {
//			final GPO gpo = newValues.next();
//			gpo.prepareBatchTerms();
//		}
//		
//		// flush terms
//		flushTerms();

        final long start = System.currentTimeMillis();
        final long count = m_dirtyGPOs.size();

        {

            /*
             * Gather up and apply the edit set (statements added and removed).
             */
            final List<Statement> inserts = new LinkedList<Statement>();
            final List<Statement> removes = new LinkedList<Statement>();
            
            final Iterator<GPO> updates = m_dirtyGPOs.iterator();

            while (updates.hasNext()) {

                updates.next().prepareBatchUpdate(inserts, removes);

            }
            
            // Atomic commit.
            flushStatements(inserts, removes);
            
        }
        
        {

            /*
             * Tell the dirty objects that they have been committed and are now
             * clean.
             */

            final Iterator<GPO> updates = m_dirtyGPOs.iterator();

            while (updates.hasNext()) {

                updates.next().doCommit();

            }

            // Clear the dirty object list.
            m_dirtyGPOs.clear();

        }
        
        if (log.isTraceEnabled())
            log.trace("Flush took " + (System.currentTimeMillis() - start)
                    + "ms for " + count + " objects");

    }
	
    /**
     * Flush statements to be inserted and removed to the backing store..
     * 
     * @param insertList
     *            The list of statements to be added.
     * @param removeList
     *            The list of statements to be removed.
     */
    abstract protected void flushStatements(final List<Statement> insertList,
            final List<Statement> removeList);

	@Override
	public IGPO getGPO(final Resource id) {

	    IGPO ret = m_dict.get(id);
		
		if (ret == null) {

            final IGPO tmp = m_dict.putIfAbsent(id, ret = new GPO(this, id));

            if (tmp != null) {
            
                // Lost the data race.
                ret = tmp;
                
            }

		}
		
		return ret;

	}

    /**
     * {@inheritDoc}
     * 
     * FIXME This is using the {@link String} representation of the
     * {@link Statement} as the blank node ID. It needs to work with the stable
     * {@link IV}s as assigned by the lexicon. However, we need to ensure that
     * the {@link IV}s are being reported through to the object manager in the
     * various interchange formats that it uses and work through how we will
     * provide that information in the SELECT query as well as CONSTRUCT and
     * DESCRIBE (basically, we need to conneg for a MIME Type that supports it).
     */
	public IGPO getGPO(final Statement stmt) {

        final BigdataBNode id = m_valueFactory.createBNode(stmt.toString());

        // Flag indicating that this GPO is a Statement.
        id.setStatementIdentifier(true);
        
        IGPO ret = m_dict.get(id);
        
        if (ret == null) {

            final IGPO tmp = m_dict.putIfAbsent(id, ret = new GPO(this, id,
                    stmt));

            if (tmp != null) {
            
                // Lost the data race.
                ret = tmp;
                
            }

        }
        
        return ret;

    }

	public Iterator<WeakReference<IGPO>> getGPOs() {
	    
	    return m_dict.iterator();
	    
	}
	
    @Override
    public void materialize(final IGPO gpo) {
        
        if (gpo == null)
            throw new IllegalArgumentException();

        if (log.isTraceEnabled())
            log.trace("Materializing: " + gpo.getId());
        
        ((GPO) gpo).dematerialize();

        if (true) {
            
            materializeWithDescribe(gpo);
            
        } else {
         
            materializeWithSelect(gpo);
            
        }
        
    }

    long m_materialized = 0;
    
    protected void materializeWithDescribe(final IGPO gpo) {

        final String query = "DESCRIBE <" + gpo.getId().toString() + ">";

        initGPO((GPO) gpo, evaluateGraph(query));
        
        /**
         * 
         */
        m_materialized++;
        
        if (m_materialized % 10000 == 0)
        	System.out.println("Materialized: " + m_materialized + ", dictionary: " + m_dict.size() + ", m_dirtyGPOs: " + m_dirtyGPOs.size());

    }

    protected void materializeWithSelect(final IGPO gpo) {

        final String query = "SELECT ?p ?v WHERE {<" + gpo.getId().toString()
                + "> ?p ?v}";

        final ICloseableIterator<BindingSet> res = evaluate(query);

        while (res.hasNext()) {

            final BindingSet bs = res.next();

            ((GPO) gpo).initValue((URI) bs.getValue("p"), bs.getValue("v"));

        }

    }
    
    public Map<Resource, IGPO> initGPOs(final ICloseableIterator<Statement> itr) {
        
        return initGPO(null/* gpo */, itr);

    }
    
    /**
     * Initialize one or more {@link IGPO}s from a collection of statements.
     * 
     * @param gpo
     *            The gpo (optional). When given, only the specified
     *            {@link IGPO} will be initialized. When not provided, all
     *            {@link Resource}s in the subject and object position of the
     *            visited {@link Statement}s will be resolved to {@link IGPO}s
     *            and the corresponding properties and/or links initialized from
     *            the {@link Statement}s.
     * @param stmts
     *            The statements.
     * 
     * @return A hard reference collection that will keep the any materialized
     *         {@link IGPO}s from being finalized before the caller has a chance
     *         to do something with them.
     */
    protected Map<Resource, IGPO> initGPO(final GPO gpo,
            final ICloseableIterator<Statement> stmts) {

        final Map<Resource, IGPO> map;

        if (gpo != null) {

            map = Collections.singletonMap((Resource) gpo.getId(), (IGPO) gpo);

        } else {
            
            map = new HashMap<Resource, IGPO>();
            
        }

        try {

            final Resource id = gpo == null ? null : gpo.getId();
            
            int statements = 0;

            while (stmts.hasNext()) {

                final Statement stmt = stmts.next();
                final Resource subject = stmt.getSubject();
                final URI predicate = stmt.getPredicate();
                final Value value = stmt.getObject();

                if (id != null) {

                    /*
                     * Initializing some specific gpo provided by the caller.
                     */
                    
                    if (subject.equals(id)) {

                        // property or link out.
                        gpo.initValue(predicate, value);

                    } else { // links in - add to LinkSet

                        gpo.initLinkValue(predicate, subject);

                    }

                } else {
                
                    /*
                     * Initial GPOs for all resources visited.
                     */
                    {

                        final GPO tmp = (GPO) getGPO(subject);

                        // property or link out.
                        tmp.initValue(predicate, value);
                        
                        map.put(tmp.getId(), tmp);
                        
                    }

                    if(value instanceof Resource) {
                        
                        final GPO tmp = (GPO) getGPO((Resource) value);

                        // Link in.
                        tmp.initLinkValue(predicate, subject);
                        
                        map.put(tmp.getId(), tmp);
                        
                    }
                    
                }
                
                statements++;

            }

            if (log.isTraceEnabled())
                log.trace("Materialized: " + (gpo == null ? "null" : gpo.getId()) + " with "
                        + statements + " statements");

            return map;
            
        } finally {

            stmts.close();

        }

    }

	@Override
	public int beginNativeTransaction() {
        lock.lock();
        try {
            return m_transactionCounter++;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int commitNativeTransaction(final int expectedCounter) {
        lock.lock();
        try {

            final int ret = --m_transactionCounter;
            
            if (ret != expectedCounter) {

                throw new IllegalArgumentException(
                        "Unexpected transaction counter");
                
            }

            if (ret == 0) {

                flushDirtyObjects();

//                doCommit();
                
            }

            return ret;
            
        } finally {
            lock.unlock();
        }
	}

//    /**
//     * Hook for extended commit processing.
//     */
//	protected abstract void doCommit();

    @Override
    public int getNativeTransactionCounter() {
        /*
         * Note: You must obtain the lock for visibility of the current value
         * unless the transaction counter is either volatile or an
         * AtomicInteger.
         */
        lock.lock();
        try {
            return m_transactionCounter;
        } finally {
            lock.unlock();
        }
	}

	@Override
    public void rollbackNativeTransaction() {
        lock.lock();
        try {
            clearCache();
            m_transactionCounter = 0;
            if (m_idGenerator != null) {
                m_idGenerator.rollback();
            }
//            doRollback();
        } finally {
            lock.unlock();
        }
	}

//	/**
//	 * Hook for extended rollback processing.
//	 */
//	abstract protected void doRollback();

	@Override
	public IGPO createGPO() {
		
		final Resource uri = m_idGenerator.genId();
		
//		addNewTerm((BigdataValue) uri);

		final GPO ret = (GPO) getGPO(uri);
		
		ret.setMaterialized(true);
		
		return ret;
	}

    @Override
    final public void remove(final IGPO gpo) {

        gpo.remove();
        
    }

    /**
	 * Simple save/recall interface that the ObjectManager provides to simplify
	 * other pattern implementations.  Internally it uses a NameManager GPO
	 */
    @Deprecated // no need for explicit save/recall.
	public void save(final URI key, Value value) {

        getGPO(s_nmeMgr).setValue(key, value);
        
	}

	/**
	 * Simple save/recall interface that the ObjectManager provides to simplify
	 * other pattern implementations.  Internally it uses a NameManager GPO
	 */
    @Deprecated // no need for explicit recall.
	public Value recall(final URI key) {	
		return getGPO(s_nmeMgr).getValue(key);
	}
	
	@Deprecated // no need for explicit recall.
	public IGPO recallAsGPO(final URI key) {
		
	    final Value val = recall(key);
		
		if (val instanceof Resource) {
			return getGPO((Resource) val);
		} else {
			return null;
		}
	}

	/**
	 * Return the list of names that have been used to save references. These
	 * are the properties of the internal NameManager.
	 */
	public Iterator<URI> getNames() {

	    final GPO nmgr = (GPO) getGPO(s_nmeMgr);
		
		return nmgr.getPropertyURIs();
	}

    @Override
    public void close() {

        clearCache();
        
    }
    
	final public void clearCache() {

	    m_dict.clear();
        m_dirtyGPOs.clear();
        
	}

    /**
     * Encode a URL, Literal, or blank node for inclusion in a SPARQL query to
     * be sent to the remote service.
     * 
     * @param v
     *            The resource.
     *            
     * @return The encoded representation of the resource.
     * 
     *         TODO This must correctly encode a URL, Literal, or blank node for
     *         inclusion in a SPARQL query to be sent to the remote service.
     */
    public String encode(final Resource v) {
        
        return v.stringValue();
        
    }
    
}
