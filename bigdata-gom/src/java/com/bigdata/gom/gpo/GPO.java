package com.bigdata.gom.gpo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.repository.RepositoryException;

import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.ObjectMgrModel;
import com.bigdata.gom.skin.GenericSkinRegistry;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteralImpl;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.striterator.ICloseableIterator;

/**
 * A TripleStore backed GPO yields a number of challenges.
 * 
 * <ol>
 * <li> We need a property mechanism to track the statement assertion
 * and retractions required for efficient update.  The GPOEntry class
 * provides support for this with its linked lists of GPOValues, also 
 * supporting multiple values per predicate.</li>
 * 
 * <li>A strategy for lazy materialization.  This turns out to be fairly
 * straightforward, with the GPO created unmaterialized and requesting
 * as necessary</li>
 * 
 * <li>Most importantly we have a consistency/referential integrity
 * issue since the GPO state is not kept in sync with the underlying
 * triple store.  The simple solution is to ensure that any update
 * is immediately pushed through to the underlying store, but this
 * loses the advantage of the localised cache.  Although it could be
 * argued that a general query is made against committed data the
 * same case cannot be made when building GPO models.  It seems we
 * need a mechanism to lazily flush updates relevant to a specific
 * property.</li>
 * 
 * <li>An alternate solution is to view the ObjectManager transactions
 * as transactional updates to the GPO structures and that when these
 * are committed then the model is consistent.  Separating the
 * GPO commit from any underlying TripleStore commit. So the GPO processing
 * inside the GPO transaction is restricted and referential integrity
 * is not maintained.  This is similar to the read-committed query
 * semantics of most databases, which is not to commend it in any way.  I am
 * keen to find a solution to this.</li>
 * 
 * <li>One option is to force GPO related
 * updates through to the underlying store, not necessarily a problem since
 * a rollback already performs a TripleStore abort anyhow.</li>
 * </ol>
 * 
 * @author Martyn Cutcher
 *
 */
public class GPO implements IGPO {

	private static final Logger log = Logger.getLogger(GPO.class);
	
	final ObjectMgrModel m_om;
	final Resource m_id;
	
	boolean m_materialized = false;
	
	boolean m_clean = true;
	
	ArrayList<IGenericSkin> m_skins = null;
	
	static class LinkValue {
		final Value m_value;
		LinkValue m_next;
		
		LinkValue(Value value) {
			m_value = value;
		}
	}
	/**
	 * The GPOEntry retains the state necessary for providing delta updates to the underlying
	 * triple data.  It supports multi-values against the same property and records values
	 * removed and added.
	 */
	static class GPOEntry {
		final URI m_key;
		GPOEntry m_next;
		
		LinkValue m_values;
		LinkValue m_addedValues;
		LinkValue m_removedValues;
		
		GPOEntry(final URI key) {
			assert key != null;
			
			m_key = key;
		}


		/**
		 * initValue is called by ObjectManager materialize and resets the GPO
		 * to its read state. Therefore the value is added to the m_values
		 * list and not m_addedValues.
		 */
		public void initValue(Value value) {
			final LinkValue newValue = new LinkValue(value);
			newValue.m_next = m_values;
			m_values = newValue;
			
		}
		
		public void set(final Value value) {
			m_addedValues = new LinkValue(value);
			
			// move m_values to m_removedValues
			LinkValue nxt = m_values;
			while (nxt != null) {
				LinkValue rem = nxt;
				nxt = nxt.m_next;
				
				rem.m_next = m_removedValues;
				m_removedValues = rem;
			}
			// and clear any current values
			m_values = null;
		}

		public Iterator<Value> values() {
			return new Iterator<Value>() {
				LinkValue m_cur = m_values;
				LinkValue m_added = m_addedValues;
				@Override
				public boolean hasNext() {
					return m_cur != null || m_added != null;
				}

				@Override
				public Value next() {
					final LinkValue ret = m_cur != null ? m_cur : m_added;
					if (ret == null) {
						throw new NoSuchElementException();
					}
					if (m_cur != null) {
						m_cur = m_cur.m_next;
					} else {
						m_added = m_added.m_next;
					}
					
					return ret.m_value;
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
				
			};
		}

		static class ValueIterator implements Iterator<Value> {
			LinkValue m_cur;
			
			ValueIterator(final LinkValue cur) {
				m_cur = cur;
			}
			@Override
			public boolean hasNext() {
				return m_cur != null;
			}

			@Override
			public Value next() {
				final LinkValue ret = m_cur;
				m_cur = m_cur.m_next;
				
				return ret.m_value;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		}
		
		public Iterator<Value> removes() {
			Iterator<Value> ret = new ValueIterator(m_removedValues);
			return ret;
		}
		
		public Iterator<Value> additions() {
			Iterator<Value> ret = new ValueIterator(m_addedValues);
			return ret;
		}

		public Value getValue() {
			if (m_values != null) {
				return m_values.m_value;
			} else if (m_addedValues != null) {
				return m_addedValues.m_value;
			} else {
				return null;
			}
		}


		/**
		 * Committing the entry checks for any added values and
		 * moves this to the values chain.
		 */
		public void commit() {
			if (m_addedValues != null) {
				if (m_values == null) {
					m_values = m_addedValues;
				} else {
					LinkValue tail = m_values;
					while (tail.m_next != null)
						tail = tail.m_next;
					tail.m_next = m_addedValues;
				}
				m_addedValues = null;
			}
			m_removedValues = null;		
		}

		/**
		 * A new value is only added if it does not already exist, ensuring the GPO maintains
		 * semantics with the underlying TripleStore.
		 * 
		 * @return true if value was added
		 */
		public boolean add(final Value value) {
			final Iterator<Value> values = values();
			while (values.hasNext()) {
				if (values.next().equals(value)) {
					return false;
				}
			}
			
			final LinkValue nv = new LinkValue(value);
			nv.m_next = m_addedValues;
			m_addedValues = nv;
			
			return true;
		}


		public boolean hasValues() {
			return m_values != null || m_addedValues != null;
		}
	}
	GPOEntry m_headEntry = null;
	GPOEntry m_tailEntry = null;
	
	GPOEntry establishEntry(final URI key) {
		final URI fkey = m_om.internKey(key);
		if (m_headEntry == null) {
			m_headEntry = m_tailEntry = new GPOEntry(fkey);
		} else {
			GPOEntry entry = m_headEntry;
			while (entry != null) {
				if (entry.m_key == fkey) {
					return entry;
				}
				entry = entry.m_next;
			}
			m_tailEntry = m_tailEntry.m_next = new GPOEntry(fkey);
		}
		
		return m_tailEntry;
	}
	
	/**
	 * The entry is interned to provide a unique Object and allow
	 * '==' testing.
	 * 
	 * @param key
	 * @return the found entry, if any
	 */
	GPOEntry getEntry(final URI key) {
		final URI fkey = m_om.internKey(key);
		GPOEntry entry = m_headEntry;
		while (entry != null) {
			if (entry.m_key == fkey) {
				return entry;
			}
			entry = entry.m_next;
		}
		return null;
	}
	
	public void reset() {
		m_headEntry = m_tailEntry = null;
	}
	
	public GPO(IObjectManager om, Resource id) {
		m_om = (ObjectMgrModel) om;
		m_id = om.internKey((URI) id);
	}
	
	@Override
	public IGenericSkin asClass(Class theClassOrInterface) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Resource getId() {
		return m_id;
	}

	/**
	 * getLinksIn simply filters the values for Resources and returns a
	 * wrapper.
	 */
	@Override
	public Set<IGPO> getLinksIn() {
		// Does not require full materialization!

		final String query = "SELECT ?x WHERE {?x ?p <" + getId().toString() + ">}";
		final ICloseableIterator<BindingSet> res = m_om.evaluate(query);
		
		final HashSet<IGPO> ret = new HashSet<IGPO>();
		while (res.hasNext()) {
			final BindingSet bs = res.next();
			ret.add(m_om.getGPO((Resource) bs.getBinding("x").getValue()));
		}
		
		return ret;
	}
	
    /** All ?y where (?y,p,self). */
	@Override
	public ILinkSet getLinksIn(URI property) {
		return new LinkSet(this, property, true);
	}

	/** All ?y where (self,?,?y). */
	@Override
	public Set<IGPO> getLinksOut() {
		materialize();
		
		final HashSet<IGPO> ret = new HashSet<IGPO>();
		GPOEntry entry = m_headEntry;
		while (entry != null) {
			Iterator<Value> values = entry.values();
			while (values.hasNext()) {
				final Value value = values.next();
				if (value instanceof Resource) {
					ret.add(m_om.getGPO((Resource) value));
				}
			}
			entry = entry.m_next;
		}
		
		return ret;
	}

	@Override
	public ILinkSet getLinksOut(URI property) {		
		materialize();
		
		return new LinkSet(this, property, false);
	}

	@Override
	public IObjectManager getObjectManager() {
		return m_om;
	}

	@Override
	public Map<URI, Long> getReverseLinkProperties() {
		materialize();
		
		final Map<URI, Long> ret = new HashMap<URI, Long>();

		final String query = "SELECT ?p (COUNT(?o) AS ?count) WHERE { ?o ?p <" + getId().toString() + "> } GROUP BY ?p";
		
		final ICloseableIterator<BindingSet> res = m_om.evaluate(query);
		
		while (res.hasNext()) {
			final BindingSet bs = res.next();
			final URI pred = (URI) bs.getBinding("p").getValue();
			final Long count = ((BigdataLiteralImpl) bs.getBinding("count").getValue()).longValue();
			ret.put(pred, count);
		}

		return ret;
	}

	@Override
	public Set<Statement> getStatements() {
		materialize();
		
		final HashSet<Statement> out = new HashSet<Statement>();
		
		GPOEntry entry = m_headEntry;
		while (entry != null) {
			Iterator<Value> values = entry.values();
			while (values.hasNext())
				out.add(makeStatement(m_id, entry.m_key, values.next()));
			
			entry = entry.m_next;
		}

		return out;
	}

	private Statement makeStatement(final Resource id, final URI key, final Value value) {
		return m_om.getValueFactory().createStatement(id, key, value);
	}

	@Override
	public Value getValue(final URI property) {
		materialize();
		
		final GPOEntry entry = getEntry(property);
		return entry != null ? entry.getValue() : null;
	}

	@Override
	public Set<Value> getValues(URI property) {
		materialize();
		
		return null;
	}

	@Override
	public boolean isBound(URI property) {
		materialize();
		
		return getValue(property) != null;
	}

	@Override
	public boolean isMemberOf(ILinkSet linkSet) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void remove() {
		m_om.remove(this);
	}

	public void dematerialize() {
		m_materialized = false;
		m_clean = true;
		m_headEntry = m_tailEntry = null;
	}

	public void initValue(URI predicate, Value object) {
		assert !m_materialized;
		
		final GPOEntry entry = establishEntry(predicate);
		entry.initValue(object);
	}
	
	@Override
	public void setValue( final URI property, final Value newValue) {
		materialize();
		
		final GPOEntry entry = establishEntry(property);
		entry.set(newValue);
		
		if (false && newValue instanceof Resource) {
			try {
				update(entry);
			} catch (RepositoryException e) {
				throw new RuntimeException("Unable to update", e);
			}
		} else {
			dirty();
		}
	}

	private void dirty() {
		if (m_clean) {
			m_clean = false;
			((ObjectMgrModel) m_om).addToDirtyList(this);
		}
	}

	@Override
	public IGPO asGeneric() {
		return this;
	}

	@Override
	public String pp() {
		materialize();
		
		final StringBuilder out = new StringBuilder("ID: " + m_id.stringValue() + "\n");
		
		GPOEntry entry = m_headEntry;
		while (entry != null) {
			Iterator<Value> values = entry.values();
			while (values.hasNext())
				out.append(entry.m_key.toString() + ": " + values.next().toString() + "\n");
			
			entry = entry.m_next;
		}
		
		return out.toString();
	}

	@Override
	public IGPO getType() {
		materialize();
		
		final URI tid = (URI) getValue(new URIImpl("attr:/type"));
		if (tid != null) {
			return m_om.getGPO(tid);
		} else {
			return null;
		}
	}

	/**
	 * Called by the ObjectManager when flushing dirty objects.  This can occur
	 * incrementally or on ObjectManager commit.  
	 * 
	 * The object is marked as clean once written.
	 * 
	 * @throws RepositoryException
	 */
	public void update() throws RepositoryException {
		assert m_materialized;
		
		GPOEntry entry = m_headEntry;
		while (entry != null) {
			update(entry);
			
			entry = entry.m_next;
		}
		
		m_clean = true;
	}
	
	private void update(final GPOEntry entry) throws RepositoryException {
		assert m_materialized;
		
		final Iterator<Value> removes = entry.removes();
		while (removes.hasNext()) {
			m_om.retract(m_id, entry.m_key, removes.next());
		}
		
		final Iterator<Value> inserts = entry.additions();
		while (inserts.hasNext()) {
			m_om.insert(m_id, entry.m_key, inserts.next());
		}
		
		entry.commit();
	}
	
	/**
	 * Basis for lazy materialization, checks materialize state and if false
	 * requests matierialization from the ObjectManager
	 */
	public void materialize() {
		if (!m_materialized) {
			synchronized (this) {
				if (!m_materialized) {
					m_om.materialize(this);
					m_materialized = true;
				}
			}
		}
	}

	@Override
	public void addValue(final URI property, final Value value) {
		materialize();
		
		final GPOEntry entry = establishEntry(property);
		if (entry.add(value)) {
			dirty();
		}
	}

	public void setMaterialized(boolean b) {
		m_materialized = true;
	}

	public void prepareBatchTerms() {
		final ObjectMgrModel oom = (ObjectMgrModel) m_om;
		GPOEntry entry = m_headEntry;
		while (entry != null) {
			final Iterator<Value> inserts = entry.additions();
			while (inserts.hasNext()) {
				final Value v = inserts.next();
				oom.checkValue(v);
			}
			
			entry = entry.m_next;
		}
	}
	
	/**
	 * adds statements for batch update
	 */
	public void prepareBatchUpdate() {
		final ObjectMgrModel oom = (ObjectMgrModel) m_om;
		GPOEntry entry = m_headEntry;
		while (entry != null) {
			final Iterator<Value> inserts = entry.additions();
			while (inserts.hasNext()) {
				final Value v = inserts.next();
				oom.insertBatch(m_id, entry.m_key, v);
			}
			final Iterator<Value> removes = entry.removes();
			while (removes.hasNext()) {
				final Value v = removes.next();
				oom.removeBatch(m_id, entry.m_key, v);
			}
			
			entry = entry.m_next;
		}
	}

	/**
	 * The getSkin method is inspired somewhat by the Microsoft Win32 getInterface
	 * that allowed an object to return multiple interfaces.  The difference with
	 * the GPO skin is that the skin should be able to interact with any underlying
	 * GPO object.
	 * 
	 * <p>It may be worthwhile performance wise to cache a skin.  I believe of more
	 * importance is to preserve identity - even of the interface/skin object.
	 * 
	 * @param skin interface required
	 * @return a skin if available
	 */
	public IGenericSkin getSkin(final Class intf) {
		IGenericSkin ret = null;
		if (m_skins != null) {
			for (int i = 0; i < m_skins.size(); i++) {
				if (intf.isInstance(m_skins.get(i))) {
					return m_skins.get(i);
				}
			}
		} else {
			m_skins = new ArrayList<IGenericSkin>(2);
		}
		
		ret = GenericSkinRegistry.asClass(this, intf);	
		m_skins.add(ret);
		
		return ret;
	}

	public Iterator<URI> getPropertyURIs() {
		materialize();
		
		return new Iterator<URI>() {
			GPOEntry m_entry = next(m_headEntry);
			
			@Override
			public boolean hasNext() {
				return m_entry != null;
			}

			private GPOEntry next(final GPOEntry entry) {
				if (entry == null)
					return null;
				
				if (entry.hasValues()) {
					return entry;
				} else {
					return next(entry.m_next);
				}
			}

			@Override
			public URI next() {
				if (m_entry == null) {
					throw new NoSuchElementException();
				}
				
				final URI ret = m_entry.m_key;
				m_entry = next(m_entry.m_next);
				
				return ret;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}
}
