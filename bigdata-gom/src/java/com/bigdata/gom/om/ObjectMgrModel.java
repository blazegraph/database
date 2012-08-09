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

import java.util.ArrayList;
import java.util.Iterator;
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
import org.openrdf.repository.RepositoryException;

import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
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
     * Local cache.
     */
    private final WeakHashMap<Resource, IGPO> m_dict = new WeakHashMap<Resource, IGPO>();
	
    /**
     * TODO The {@link BigdataValueFactory} handles this with
     * {@link BigdataValueFactory#asValue(Value)}. Use that instead?
     */
	private final ConcurrentHashMap<URI, URI> m_internedKeys = new ConcurrentHashMap<URI, URI>();

    /*
     * We need to maintain a dirty list in order to pin object references that
     * are dirty. On commit, we need to send the retracts and the asserts in a
     * single operation, which is why these things are tracked on separate
     * lists.
     * 
     * FIXME The OM should not be tracking the terms. The StatementBuffer or
     * Sail will handle this.
     */
	// new terms cache to enable batch term registration on update/flush
    protected final List<BigdataValue> m_terms = new ArrayList<BigdataValue>();
    protected final List<Statement> m_inserts = new ArrayList<Statement>();
    protected final List<Statement> m_removes = new ArrayList<Statement>();

    private final ArrayList<GPO> m_dirtyGPOs = new ArrayList<GPO>();
    
    private final int m_maxDirtyListSize = 1000; // 5000; // FIXME: Init from property file   

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
		
		addNewTerm((BigdataValue) s_nmeMgr);
		
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
		
		
		final URI uri =  old != null ? old : key;
		
		if (old == null && (uri instanceof BigdataURI) && ((BigdataURI) uri).getIV() == null)
			addNewTerm((BigdataURI) uri);
		
		return uri;
	}

	/**
	 * GPOs are added to the dirty list when initially modified.
	 * 
	 * <p>The list cannot be allowed to grow unbounded since it retains a
	 * concrete reference to the GPO and OutOfMemory will occur.  The
	 * solution is to incrementally flush the dirty list.</p>
	 */
	public void addToDirtyList(GPO gpo) {
		m_dirtyGPOs.add(gpo);
		if (m_dirtyGPOs.size() > m_maxDirtyListSize) {
			if (log.isTraceEnabled())
				log.trace("Incremental flush of dirty objects");
			
			flushDirtyObjects();
		}
	}
	
	abstract void flushTerms();
	
	private void flushDirtyObjects() {
		// prepare values
		final Iterator<GPO> newValues = m_dirtyGPOs.iterator();
		while (newValues.hasNext()) {
			final GPO gpo = newValues.next();
			gpo.prepareBatchTerms();
		}
		
		// flush terms
		flushTerms();
		
		final long start = System.currentTimeMillis();
		final long count = m_dirtyGPOs.size();

		if (true) {
		    final Iterator<GPO> updates = m_dirtyGPOs.iterator();
			while (updates.hasNext()) {
				updates.next().prepareBatchUpdate();
			}
			
			flushStatements();
		} else {
			// update dirty objects	- is it worth while batching SPO[]?
		    final Iterator<GPO> updates = m_dirtyGPOs.iterator();
			while (updates.hasNext()) {
				try {
					updates.next().update();
				} catch (RepositoryException e) {
					throw new RuntimeException("Unexpected update exception", e);
				}
			}
		}
		m_dirtyGPOs.clear();
        if (log.isTraceEnabled())
            log.trace("Flush took " + (System.currentTimeMillis() - start)
                    + "ms for " + count + " objects");
	}
	
	abstract void flushStatements();

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
            }

            doCommit();

            return ret;
        } finally {
            lock.unlock();
        }
	}

    /**
     * Hook for extended commit processing.
     */
	protected abstract void doCommit();

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
        clearCache();
        m_transactionCounter = 0;
        if (m_idGenerator != null) {
            m_idGenerator.rollback();
        }
        doRollback();
	}

	/**
	 * Hook for extended rollback processing.
	 */
	abstract protected void doRollback();

	@Override
	public IGPO createGPO() {
		
		final Resource uri = m_idGenerator.genId();
		addNewTerm((BigdataValue) uri);
		final GPO ret = (GPO) getGPO(uri);
		
		ret.setMaterialized(true);
		
		return ret;
	}

	@Deprecated // Let the BigdataSail handle this.
	protected void addNewTerm(final BigdataValue uri) {
		
        if (uri.isRealIV())
            throw new IllegalArgumentException("IV already available: "
                    + uri.stringValue());

        if (log.isDebugEnabled())
            log.debug("Adding term: " + uri);

        m_terms.add(uri);
        
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

	@Deprecated // The OM should not be worrying about IVs like this.
	public void checkValue(Value newValue) {
		final BigdataValue v = (BigdataValue) newValue;
		if (!v.isRealIV()) {
			addNewTerm(v);
		}
	}

    @Override
    public void close() {

        clearCache();
        
    }
    
	public void clearCache() {
		m_dict.clear();
		m_dirtyGPOs.clear();
	}

    abstract public void insert(final Resource id, final URI key,
            final Value val) throws RepositoryException;

    abstract public void retract(final Resource id, final URI key,
            final Value val) throws RepositoryException;

    public void insertBatch(final Resource m_id, final URI bigdataURI,
            final Value v) {
        m_inserts.add(m_valueFactory.createStatement(m_id, bigdataURI, v));
    }

    public void removeBatch(final Resource m_id, final URI bigdataURI,
            final Value v) {
        m_removes.add(m_valueFactory.createStatement(m_id, bigdataURI, v));
    }

}
