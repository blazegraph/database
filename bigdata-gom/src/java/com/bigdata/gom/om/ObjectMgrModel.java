package com.bigdata.gom.om;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryException;

import com.bigdata.gom.gpo.BasicSkin;
import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;

public abstract class ObjectMgrModel implements IObjectManager {

    protected static final Logger log = Logger.getLogger(IObjectManager.class);

    final WeakHashMap<Resource, IGPO> m_dict = new WeakHashMap<Resource, IGPO>();
	
	final ConcurrentHashMap<URI, URI> m_internedKeys = new ConcurrentHashMap<URI, URI>();
	
	final ValueFactory m_valueFactory;
	
	// new terms cache to enable batch term registration on update/flush
	final ArrayList<BigdataValue> m_terms = new ArrayList<BigdataValue>();
	final ArrayList<Statement> m_inserts = new ArrayList<Statement>();
	final ArrayList<Statement> m_removes = new ArrayList<Statement>();

	// Object Creation and ID Management patterns
	final URI s_idMgr;
	final URI s_idMgrNextId;
	final URI s_nmeMgr;

	int m_transactionCounter = 0;
	
	ObjectMgrModel(final ValueFactory valueFactory) {
		m_valueFactory = valueFactory;
		s_idMgr = m_valueFactory.createURI("gpo:idMgr");
		s_idMgrNextId = m_valueFactory.createURI("gpo:idMgr#nextId");
		s_nmeMgr = m_valueFactory.createURI("gpo:nmeMgr");
		
		addNewTerm((BigdataValue) s_idMgr);
		addNewTerm((BigdataValue) s_idMgrNextId );
		addNewTerm((BigdataValue) s_nmeMgr);
	}
	
	@Override
	public URI internKey(final URI key) {
		final URI old = m_internedKeys.putIfAbsent(key, key);
		
		
		final URI uri =  old != null ? old : key;
		
		if (old == null && (uri instanceof BigdataURI) && ((BigdataURI) uri).getIV() == null)
			addNewTerm((BigdataURI) uri);
		
		return uri;
	}

	final ArrayList<GPO> m_dirtyGPOs = new ArrayList<GPO>();
	
	final int m_maxDirtyListSize = 1000; // 5000; // FIXME: Init from property file
	
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
		Iterator<GPO> newValues = m_dirtyGPOs.iterator();
		while (newValues.hasNext()) {
			final GPO gpo = newValues.next();
			gpo.prepareBatchTerms();
		}
		
		// flush terms
		flushTerms();
		
		final long start = System.currentTimeMillis();
		final long count = m_dirtyGPOs.size();

		if (true) {
			Iterator<GPO> updates = m_dirtyGPOs.iterator();
			while (updates.hasNext()) {
				updates.next().prepareBatchUpdate();
			}
			
			flushStatements();
		} else {
			// update dirty objects	- is it worth while batching SPO[]?
			Iterator<GPO> updates = m_dirtyGPOs.iterator();
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
			log.trace("Flush took " + (System.currentTimeMillis()-start) + "ms for " + count + " objects");
	}
	
	abstract void flushStatements();

	@Override
	public IGPO getGPO(final Resource id) {
		IGPO ret = m_dict.get(id);
		
		if (ret == null) {
			ret = new GPO(this, id);
			// materialize(ret); // JFDI?
			m_dict.put(id, ret);
		}
		
		return ret;
	}

	@Override
	public synchronized int beginNativeTransaction() {
		return m_transactionCounter++;
	}

	@Override
	public int commitNativeTransaction(final int expectedCounter) {
		final int ret = --m_transactionCounter;
		if (ret != expectedCounter) {
			throw new IllegalArgumentException("Unexpected transaction counter");
		}
		
		if (ret == 0) {
			flushDirtyObjects();
		}
		
		doCommit();
		
		return ret;
	}

	abstract void doCommit();

	@Override
	public int getNativeTransactionCounter() {
		return m_transactionCounter;
	}

	@Override
	public void rollbackNativeTransaction() {
		// just clear the cache for now
		m_dict.clear();
		m_dirtyGPOs.clear();
		m_transactionCounter = 0;	
		m_idMgr = null;
		
		doRollback();
	}

	abstract void doRollback();

	@Override
	public IGPO createGPO() {
		BasicSkin idMgr = getIdMgr();
		
		int nxtId = idMgr.getIntValue(s_idMgrNextId)+1;
		idMgr.setValue(s_idMgrNextId, nxtId);
		
		final Resource uri = getValueFactory().createURI("gpo:#" + nxtId);
		addNewTerm((BigdataValue) uri);
		final GPO ret = (GPO) getGPO(uri);
		
		ret.setMaterialized(true);
		
		return ret;
	}
	
	protected void addNewTerm(final BigdataValue uri) {
		if (uri.isRealIV())
			throw new IllegalArgumentException("IV already available: " + uri.stringValue());
		
		m_terms.add(uri);
	}

	BasicSkin m_idMgr = null;
	protected BasicSkin getIdMgr() {
		if (m_idMgr == null) {
			if (log.isTraceEnabled())
				log.trace("retrieving ID Manager");
			
			IGPO idMgr = getGPO(s_idMgr);
			
			m_idMgr = new BasicSkin(idMgr);
		}
		
		return m_idMgr;
	}

	/**
	 * Simple save/recall interface that the ObjectManager provides to simplify
	 * other pattern implementations.  Internally it uses a NameManager GPO
	 */
	public void save(final URI key, Value value) {
		getGPO(s_nmeMgr).setValue(key, value);
	}

	/**
	 * Simple save/recall interface that the ObjectManager provides to simplify
	 * other pattern implementations.  Internally it uses a NameManager GPO
	 */
	public Value recall(final URI key) {	
		return getGPO(s_nmeMgr).getValue(key);
	}
	
	public IGPO recallAsGPO(final URI key) {
		Value val = recall(key);
		
		if (val instanceof Resource) {
			return getGPO((Resource) val);
		} else {
			return null;
		}
	}

	public void checkValue(Value newValue) {
		final BigdataValue v = (BigdataValue) newValue;
		if (!v.isRealIV()) {
			addNewTerm(v);
		}
	}

	public void clearCache() {
		m_dict.clear();
		m_dirtyGPOs.clear();
	}

	public void insertBatch(final Resource m_id, final URI bigdataURI, final Value v) {
		m_inserts.add(m_valueFactory.createStatement(m_id, bigdataURI, v));
	}

	public void removeBatch(final Resource m_id, final URI bigdataURI, final Value v) {
		m_removes.add(m_valueFactory.createStatement(m_id, bigdataURI, v));
	}

	@Override
	public ValueFactory getValueFactory() {
		return m_valueFactory;
	}

}
