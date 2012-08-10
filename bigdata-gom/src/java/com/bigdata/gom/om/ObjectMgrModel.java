/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 19, 2012
 */
package com.bigdata.gom.om;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.rdf.model.BigdataValueFactory;

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
    
    // TODO Should this be a BigdataValueFactory?
    protected final ValueFactory m_valueFactory;
    
    /** Object Creation and ID Management patterns. */
    private final IIDGenerator m_idGenerator;

    /**
     * The "running object table." Dirty objects are wired into this table by
     * the existence.
     * 
     * TODO If people hold onto a reference to an object it will not come out of
     * this table. We need to remove all dirty objects from this map on a
     * rollback.
     * 
     * TODO This map is touched for every pointer traversal. I would prefer to
     * use weak references on the GPO forward and backward links to avoid those
     * hash table lookups.
     * 
     * TODO This collection is not thread-safe without synchronization.
     */
    private final WeakHashMap<Resource, IGPO> m_dict = new WeakHashMap<Resource, IGPO>();
	
    /**
     * This is only for the predicates.
     * 
     * TODO The {@link BigdataValueFactory} handles this with
     * {@link BigdataValueFactory#asValue(Value)}. Use that instead?
     */
	private final ConcurrentHashMap<URI, URI> m_internedKeys = new ConcurrentHashMap<URI, URI>();

    /**
     * We need to maintain a dirty list in order to pin object references that
     * are dirty. On commit, we need to send the retracts and the asserts in a
     * single operation. The GPO.GPOEntry tracks those individual asserts and
     * retracts.
     */
    private final List<GPO> m_dirtyGPOs = new LinkedList<GPO>();

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
            final ValueFactory valueFactory) {

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
		
//		addNewTerm((BigdataValue) s_nmeMgr);
		
	}
	
	public IGPO getDefaultNameMgr() {
		
	    return getGPO(s_nmeMgr);
	    
	}
	
	public UUID getID() {
		
	    return m_uuid;
	    
	}
	
    @Override
    final public ValueFactory getValueFactory() {

        return m_valueFactory;

    }

//	class DefaultIDGenerator implements IIDGenerator {
//		final URI s_idMgr;
//		final URI s_idMgrNextId;
//		BasicSkin m_idMgr;
//		
//		DefaultIDGenerator() {
//			s_idMgr = m_valueFactory.createURI("gpo:idMgr/"+m_uuid);
//			s_idMgrNextId = m_valueFactory.createURI("gpo:idMgr/"+m_uuid + "#nextId");
//			
//			m_idMgr = new BasicSkin(getGPO(s_idMgr));
//
//			addNewTerm((BigdataValue) s_idMgr);
//			addNewTerm((BigdataValue) s_idMgrNextId );
//		}
//		
//		/**
//		 * Default IIDGenerator implementation for ObjectManagers.
//		 */
//		public URI genId() {
//			if (m_idMgr == null) {
//				m_idMgr = new BasicSkin(getGPO(s_idMgr));
//			}
//			
//            final int nxtId = m_idMgr.getIntValue(s_idMgrNextId) + 1;
//
//            m_idMgr.setValue(s_idMgrNextId, nxtId);
//			
//			return getValueFactory().createURI("gpo:" + m_uuid + "/" + nxtId);
//		}
//
//		public void rollback() {
//			m_idMgr = null; // force reload to committed state on next access
//		}
//	}
	
	@Override
	public URI internKey(final URI key) {
		
        final URI old = m_internedKeys.putIfAbsent(key, key);

        final URI uri = old != null ? old : key;

//		if (old == null && (uri instanceof BigdataURI) && ((BigdataURI) uri).getIV() == null)
//			addNewTerm((BigdataURI) uri);
		
		return uri;
	}

    /**
     * GPOs are added to the dirty list when initially modified. The dirty list
     * is not bounded. Large updates should be done using the RDF and SPARQL
     * layer which do not have this implicit scaling limit.
     * 
     * TODO We can not do incremental eviction unless we are holding open a
     * connection to the database that will isolate those edits. This could be
     * done in principle with either full read/write transactions or with the
     * unisolated connection (if embedded) but we do not yet support remove
     * create/run/commit for full read/write transactions in the NSS REST API.
     */
	public void addToDirtyList(final GPO gpo) {

	    if(gpo == null)
	        throw new IllegalArgumentException();

        if (!gpo.isDirty())
            throw new IllegalStateException();

	    m_dirtyGPOs.add(gpo);
	    
//		if (m_dirtyGPOs.size() > m_maxDirtyListSize) {
//			if (log.isTraceEnabled())
//				log.trace("Incremental flush of dirty objects");
//			
//			flushDirtyObjects();
//		}

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
		
		    ret = new GPO(this, id);
		    
			m_dict.put(id, ret);
			
		}
		
		return ret;

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
