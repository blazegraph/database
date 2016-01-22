package com.bigdata.gom.gpo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;

import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.ObjectMgrModel;
import com.bigdata.gom.skin.GenericSkinRegistry;
import com.bigdata.rdf.model.BigdataLiteralImpl;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A TripleStore backed GPO yields a number of challenges.
 * 
 * <ol>
 * <li>We need a property mechanism to track the statement assertion and
 * retractions required for efficient update. The GPOEntry class provides
 * support for this with its linked lists of GPOValues, also supporting multiple
 * values per predicate.</li>
 * 
 * <li>A strategy for lazy materialization. This turns out to be fairly
 * straightforward, with the GPO created unmaterialized and requesting as
 * necessary</li>
 * 
 * <li>Most importantly we have a consistency/referential integrity issue since
 * the GPO state is not kept in sync with the underlying triple store. The
 * simple solution is to ensure that any update is immediately pushed through to
 * the underlying store, but this loses the advantage of the localised cache.
 * Although it could be argued that a general query is made against committed
 * data the same case cannot be made when building GPO models. It seems we need
 * a mechanism to lazily flush updates relevant to a specific property.</li>
 * 
 * <li>An alternate solution is to view the ObjectManager transactions as
 * transactional updates to the GPO structures and that when these are committed
 * then the model is consistent. Separating the GPO commit from any underlying
 * TripleStore commit. So the GPO processing inside the GPO transaction is
 * restricted and referential integrity is not maintained. This is similar to
 * the read-committed query semantics of most databases, which is not to commend
 * it in any way. I am keen to find a solution to this.</li>
 * 
 * <li>One option is to force GPO related updates through to the underlying
 * store, not necessarily a problem since a rollback already performs a
 * TripleStore abort anyhow.</li>
 * </ol>
 * 
 * @author Martyn Cutcher
 * 
 */
public class GPO implements IGPO {

	private static final Logger log = Logger.getLogger(GPO.class);

	// final private ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();
	
	/**
	 * When the GPO is removed m_removed is set to true and any attempt to access
	 * the IGPO methods will throw an IllegalStateException
	 */
	private boolean m_removed = false;

	/**
	 * The owning {@link IObjectManager}.
	 */
	private final ObjectMgrModel m_om;

	/**
	 * The identifier for this {@link IGPO}.
	 */
	private final BigdataResource m_id;

	private final BigdataStatement m_stmt;
	
	/**
	 * <code>true</code> iff the forward link set has been materialized.
	 */
	private volatile boolean m_materialized = false;

	private boolean m_clean = true;

	/**
	 * Head of the double-linked list of values for each predicate. There is one
	 * double-linked list of {@link GPOEntry}s and this is the head of that
	 * list. There is one {@link GPOEntry} for each predicate that either has
	 * (or had) a bound value. Each {@link GPOEntry} models all values for a
	 * given predicate for this subject and tracks the edit list for the values
	 * for that predicate.
	 */
	private GPOEntry m_headEntry = null;
	/**
	 * Tail of the double-linked list of values for each predicate. There is one
	 * double-linked list of {@link GPOEntry}s and this is the head of that
	 * list. There is one {@link GPOEntry} for each predicate that either has
	 * (or had) a bound value. Each {@link GPOEntry} models all values for a
	 * given predicate for this subject and tracks the edit list for the values
	 * for that predicate.
	 */
	private GPOEntry m_tailEntry = null;

	/**
	 * The DESCRIBE cache may eagerly load linksIn linkSets. To support this
	 * {@link GPOEntry}s can be maintained for linksIn.
	 */
	private GPOEntry m_headLinkEntry = null;
	private GPOEntry m_tailLinkEntry = null;

	private ArrayList<IGenericSkin> m_skins = null;

	/**
	 * A {@link Value} on any of the {@link GPOEntry} links (values, removed,
	 * and added). The {@link LinkValue}s are organized as a single-linked list.
	 */
	static class LinkValue {

		/**
		 * The RDF {@link Value}.
		 */
		final Value m_value;

		/**
		 * The next {@link LinkValue} in the list -or- <code>null</code> if this
		 * is the last {@link LinkValue} in the list.
		 */
		LinkValue m_next;

		LinkValue(final Value value) {

			if (value == null)
				throw new IllegalArgumentException();

			m_value = value;

		}

		public boolean contains(BigdataResource id) {
			LinkValue tst = this;
			while (tst != null) {
				if (tst.m_value.equals(id)) {
					return true;
				}
				tst = tst.m_next;				
			}
			
			return false;
		}

	}

	/**
	 * The GPOEntry retains the state necessary for providing delta updates to
	 * the underlying triple data. It supports multi-values against the same
	 * property and records values removed and added.
	 */
	static class GPOEntry {

		/**
		 * This is the predicate.
		 */
		private final URI m_key;
		/**
		 * This is the next entry for a different predicate.
		 */
		private GPOEntry m_next;
		/**
		 * This is the collection of bound values at the time the owning
		 * {@link GPO} was materialized or last committed. The total set of
		 * current bindings is {@link #m_values} PLUS {@link #m_addedValues}.
		 * 
		 * TODO We need to improve the documentation here. Ideally, I would like
		 * to see the pre-/post-conditions for add(), and remove() as well as
		 * initValue() and commit().
		 */
		private LinkValue m_values;
		/**
		 * Values added to this predicate and subject since the last commit.
		 */
		private LinkValue m_addedValues;
		/**
		 * Values removed from this predicate and subject since the last commit.
		 */
		private LinkValue m_removedValues;

		/**
		 * Keep track of entry size
		 */
		private int m_size = 0;

		GPOEntry(final URI key) {

			if (key == null)
				throw new IllegalArgumentException();

			m_key = key;

		}

		/**
		 * initValue is called by ObjectManager materialize and resets the GPO
		 * to its read state. Therefore the value is added to the m_values list
		 * and not m_addedValues.
		 */
		public void initValue(final GPO owner, final Value value) {
			final LinkValue newValue = new LinkValue(value);
			newValue.m_next = m_values;
			m_values = newValue;

			m_size++;
		}

		public void set(final GPO owner, final Value value) {

			m_addedValues = new LinkValue(value);

			// move m_values to m_removedValues
			LinkValue nxt = m_values;
			while (nxt != null) {
				final LinkValue rem = nxt;
				nxt = nxt.m_next;

				rem.m_next = m_removedValues;
				m_removedValues = rem;
			}
			// and clear any current values
			m_values = null;

			m_size = 1;
		}

		/**
		 * Remove the statement for the predicate and value if found, returning
		 * true IFF the statement was removed.
		 * 
		 * @return <code>true</code> iff the statement was removed.
		 */
		public boolean remove(final GPO owner, final Value value) {

			if (value == null)
				throw new IllegalArgumentException();

			// if on m_values then move to m_removed
			LinkValue test = m_values;
			LinkValue prev = null;
			while (test != null) {
				if (value.equals(test.m_value)) {
					if (prev == null) {
						m_values = test.m_next;
					} else {
						prev.m_next = test.m_next;
					}
					// add to removed values
					test.m_next = m_removedValues;
					m_removedValues = test;

					m_size--;

					return true;
				}
				prev = test;
				test = test.m_next;
			}

			// if on m_added then just remove it
			test = m_addedValues;
			prev = null;
			while (test != null) {
				if (value.equals(test.m_value)) {
					if (prev == null) {
						m_addedValues = test.m_next;
					} else {
						prev.m_next = test.m_next;
					}

					m_size--;

					return true;
				}
				prev = test;
				test = test.m_next;
			}

			return false;
		}

		/**
		 * Remove all statements for this predicate on the owning {@link IGPO}.
		 * 
		 * @return <code>true</code> iff any statements were removed.
		 */
		public boolean removeAll(final GPO owner) {
			if (m_size == 0) {
				return false;
			}

			// just move m_values to tail of m_removedValues
			if (m_removedValues == null) {
				m_removedValues = m_values;
			} else {
				LinkValue tail = m_removedValues;
				while (tail.m_next != null)
					tail = tail.m_next;
				tail.m_next = m_values;
			}

			m_values = null; // clear values
			m_addedValues = null; // remove any additions

			m_size = 0;

			return true;
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
		 * Committing the entry checks for any added values and moves this to
		 * the values chain.
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
		 * A new value is only added if it does not already exist, ensuring the
		 * GPO maintains semantics with the underlying TripleStore.
		 * 
		 * @return true if value was added
		 */
		public boolean add(final GPO owner, final Value value) {
			final Iterator<Value> values = values();
			while (values.hasNext()) {
				if (values.next().equals(value)) {
					return false;
				}
			}

			final LinkValue nv = new LinkValue(value);
			nv.m_next = m_addedValues;
			m_addedValues = nv;

			m_size++;

			return true;
		}

		public boolean hasValues() {
			return m_values != null || m_addedValues != null;
		}

		public int size() {
			return m_size;
		}

		public boolean contains(BigdataResource id) {
			return (m_values != null && m_values.contains(id))
				   || (m_addedValues != null && m_addedValues.contains(id));
		}
	}

	/**
	 * Make sure that there is a {@link GPOEntry} for that property. If one is
	 * found, return it. Otherwise create and return a new {@link GPOEntry}.
	 * 
	 * @param key
	 *            The property.
	 * @return The {@link GPOEntry} and never <code>null</code>.
	 */
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
	 * Make sure that there is a {@link GPOEntry} for that link property. If one
	 * is found, return it. Otherwise create and return a new {@link GPOEntry}.
	 * 
	 * @param key
	 *            The link property.
	 * @return The {@link GPOEntry} and never <code>null</code>.
	 */
	GPOEntry establishLinkEntry(final URI key) {
		final URI fkey = m_om.internKey(key);
		if (m_headLinkEntry == null) {
			m_headLinkEntry = m_tailLinkEntry = new GPOEntry(fkey);
		} else {
			GPOEntry entry = m_headLinkEntry;
			while (entry != null) {
				if (entry.m_key == fkey) {
					return entry;
				}
				entry = entry.m_next;
			}
			m_tailLinkEntry = m_tailLinkEntry.m_next = new GPOEntry(fkey);
		}

		return m_tailLinkEntry;
	}

	/**
	 * The entry is interned to provide a unique Object and allow '==' testing.
	 * 
	 * @param key
	 * @return the found entry, if any
	 */
	GPOEntry getEntry(final URI key) {
		checkLive();

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

	/**
	 * The entry is interned to provide a unique Object and allow '==' testing.
	 * 
	 * @param key
	 * @return the found link entry, if any
	 */
	GPOEntry getLinkEntry(final URI key) {
		checkLive();

		final URI fkey = m_om.internKey(key);
		GPOEntry entry = m_headLinkEntry;
		while (entry != null) {
			if (entry.m_key == fkey) {
				return entry;
			}
			entry = entry.m_next;
		}
		return null;
	}

	public void dematerialize() {
		m_materialized = false;
		m_clean = true;
		m_headEntry = m_tailEntry = null;
	}
	
    public int hashCode() {
    	return m_id.hashCode();
    }

    public GPO(final IObjectManager om, final Resource id) {

		if (om == null)
			throw new IllegalArgumentException();

		if (id == null)
			throw new IllegalArgumentException();

		m_om = (ObjectMgrModel) om;

		m_id = om.getValueFactory().asValue(id);
		
		m_stmt = null;

	}
	
    public GPO(final IObjectManager om, final BNode id, final Statement stmt) {

        if (om == null)
            throw new IllegalArgumentException();

        if (id == null)
            throw new IllegalArgumentException();

        m_om = (ObjectMgrModel) om;

        m_id = om.getValueFactory().asValue(id);
        
        final BigdataStatement stmt2 = om.getValueFactory().createStatement(
                ((ObjectMgrModel)om).bestEffortIntern(stmt.getSubject()),//
                ((ObjectMgrModel)om).internKey(stmt.getPredicate()), //
                ((ObjectMgrModel)om).bestEffortIntern(stmt.getObject()) //
                );

        this.m_stmt = stmt2;
        
    }

	private void checkLive() {
		if (m_removed)
			throw new IllegalStateException("This GPO has been removed");
	}

	@Override
	public IGenericSkin asClass(Class theClassOrInterface) {
		/**
		 * Could simply call through to the GenericSkinRegistry
		 * 
		 * GenericSkinRegistry.asClass(this, theClassOrInterface);
		 * 
		 * but this ignores any cached skins already created.
		 */
		return getSkin(theClassOrInterface);
	}

	@Override
	public IObjectManager getObjectManager() {
		checkLive();

		return m_om;
	}

	@Override
	public BigdataResource getId() {
		checkLive();
		
		return m_id;
	}

    /**
     * Iff this {@link IGPO} represents a statement (aka link), then return that
     * {@link Statement}.
     * 
     * @return The {@link Statement} or <code>null</code> if the {@link IGPO}
     *         does not represent a {@link Statement}.
     */
    public BigdataStatement getStatement() {
	    
	    return m_stmt;
	    
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
	 * @see ObjectMgrModel#encode(Resource)
	 */
	public String encode(final Resource v) {
		checkLive();

		return m_om.encode(v);

	}

	/**
	 * getLinksIn simply filters the values for Resources and returns a wrapper.
	 */
	@Override
	public Set<IGPO> getLinksIn() {
	
		materialize();
		
		final HashSet<IGPO> ret = new HashSet<IGPO>();
		GPOEntry linkEntry = this.m_headLinkEntry;
		while (linkEntry != null) {
			Iterator<Value> inlinks = linkEntry.values();
			while (inlinks.hasNext()) {
				final IGPO mem = m_om.getGPO((Resource) inlinks.next());
				ret.add(mem);
			}
			linkEntry = linkEntry.m_next;
		}
		
		return ret;
		
		/*

		final String query = "SELECT ?x WHERE {?x ?p <" + encode(getId())
				+ ">}";
		final ICloseableIterator<BindingSet> res = m_om.evaluate(query);

		final HashSet<IGPO> ret = new HashSet<IGPO>();
		while (res.hasNext()) {
			final BindingSet bs = res.next();
			ret.add(m_om.getGPO((Resource) bs.getBinding("x").getValue()));
		}

		return ret;
		*/
	}

	/** All ?y where (?y,p,self). */
	@Override
	public ILinkSet getLinksIn(final URI property) {
		// materialize fully load links in from Describe cache
		materialize();

		return new LinkSet(this, property, true/* linksIn */);

	}

	/** All ?y where (self,?,?y). */
	@Override
	public Set<IGPO> getLinksOut() {
//		final Lock readLock = m_lock.readLock();
//		readLock.lock();
//		try {

			materialize();

			final HashSet<IGPO> ret = new HashSet<IGPO>();
			GPOEntry entry = m_headEntry;
			while (entry != null) {
				final Iterator<Value> values = entry.values();
				while (values.hasNext()) {
					final Value value = values.next();
					if (value instanceof Resource) {
						ret.add(m_om.getGPO((Resource) value));
					}
				}
				entry = entry.m_next;
			}

			return ret;
//		} finally {
//			readLock.unlock();
//		}
	}

	/** All ?y where (self,p,?y). */
	@Override
	public ILinkSet getLinksOut(final URI property) {

		materialize();

		return new LinkSet(this, property, false/* linksOut */);
	}

	@Override
	public Map<URI, Long> getReverseLinkProperties() {

		materialize();

		final Map<URI, Long> ret = new HashMap<URI, Long>();

		final String query = "SELECT ?p (COUNT(?o) AS ?count)\n"
				+ "WHERE { ?o ?p <" + getId().toString() + "> }\n"
				+ "GROUP BY ?p";

		final ICloseableIterator<BindingSet> res = m_om.evaluate(query);

		while (res.hasNext()) {
			final BindingSet bs = res.next();
			final URI pred = (URI) bs.getBinding("p").getValue();
			final Long count = ((Literal) bs.getBinding("count")
					.getValue()).longValue();
			ret.put(pred, count);
		}

		return ret;
	}

	@Override
	public Set<Statement> getStatements() {
//		final Lock readLock = m_lock.readLock();
//		readLock.lock();
//		try {
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
//		} finally {
//			readLock.unlock();
//		}
	}

	private Statement makeStatement(final Resource id, final URI key,
			final Value value) {
		return m_om.getValueFactory().createStatement(id, key, value);
	}

	@Override
	public Value getValue(final URI property) {
//		final Lock readLock = m_lock.readLock();
//		readLock.lock();
//		try {
			materialize();

			final GPOEntry entry = getEntry(property);
			return entry != null ? entry.getValue() : null;
//		} finally {
//			readLock.unlock();
//		}
	}

	@Override
	public Set<Value> getValues(URI property) {
//		final Lock readLock = m_lock.readLock();
//		readLock.lock();
//		try {
			materialize();

			HashSet<Value> values = new HashSet<Value>();

			final GPOEntry entry = getEntry(property);
			if (entry != null) {
				Iterator<Value> vals = entry.values();
				while (vals.hasNext()) {
					values.add(vals.next());
				}
			}

			return values;
//		} finally {
//			readLock.unlock();
//		}
	}

	@Override
	public boolean isBound(URI property) {
		materialize();

		return getValue(property) != null;
	}

	@Override
	public boolean isMemberOf(ILinkSet linkSet) {

		return linkSet.contains(this);
	}

	/**
	 * FIXME This should run a query (unless it is fully materialized, including
	 * the reverse links and forward links) and build an edit list for the
	 * retracts. [There could be a separate operation to remove only those
	 * properties associated with a skin.]
	 */
	@Override
	public void remove() {
		materialize();
		
		m_removed = true;
		
		
		// remove all properties
		GPOEntry entry = m_headEntry;
		while (entry != null) {
			clearReverseLinkSetEntries(entry);
			entry.removeAll(this);
			entry = entry.m_next;
		}
		
		// remove all linksest
		entry = m_headLinkEntry;
		while (entry != null) {
			entry.removeAll(this);
			entry = entry.m_next;
		}
		
		dirty();	
	}

	public void initValue(final URI predicate, final Value object) {

		if (predicate == null)
			throw new IllegalArgumentException();

		if (object == null)
			throw new IllegalArgumentException();

		assert !m_materialized;

		final GPOEntry entry = establishEntry(predicate);

		entry.initValue(this, object);

	}

	public void initLinkValue(final URI predicate, final Resource object) {

		if (predicate == null)
			throw new IllegalArgumentException();

		if (object == null)
			throw new IllegalArgumentException();

		assert !m_materialized;

		final GPOEntry entry = establishLinkEntry(predicate);

		entry.initValue(this, object);

	}

	@Override
	public void setValue(final URI property, final Value newValue) {

		if (property == null)
			throw new IllegalArgumentException();

		if (newValue == null)
			throw new IllegalArgumentException();

//		final Lock writeLock = m_lock.writeLock();
//		writeLock.lock();
//		try {
			materialize();
			
			// calls reverse removeLinkSetMembers
			removeValues(property);

			final GPOEntry entry = establishEntry(property);

			entry.set(this, newValue);
			
			if (newValue instanceof Resource) {
				// add to LinkSet of target
				((GPO) m_om.getGPO((Resource) newValue)).addLinkSetMember(property, m_id);
			}

			dirty();
//		} finally {
//			writeLock.unlock();
//		}
	}

    /**
     * {@inheritDoc}
     * 
     * FIXME This should be conditional, returning null if the link does not
     * exist. However, that conditional test needs to be efficient.
     */
    @Override
    public IGPO getLink(final URI property, final IGPO target) {

//        if (getValue(property) != target.getId()) {
//
//            // Link does not exist.
//            return null;
//            
//        }
    	
    	final GPOEntry entry = getEntry(property);
     	if (entry != null && entry.contains(target.getId())) {
            return m_om.getGPO(new StatementImpl(m_id, property, target.getId()));
    	}
    
        return null;

    }

//    @Override
//    public void addLink(final URI property, final IGPO target) {
//
//        addValue(property, target.getId());
//
//    }
	
	@Override
	public void addValue(final URI property, final Value value) {

		if (property == null)
			throw new IllegalArgumentException();

		if (value == null)
			throw new IllegalArgumentException();

//		final Lock writeLock = m_lock.writeLock();
//		writeLock.lock();
//		try {
			materialize();

			final GPOEntry entry = establishEntry(property);

			if (entry.add(this, value)) {

				dirty();

			}

//		} finally {
//			writeLock.unlock();
//		}
	}

	/**
	 * The under the hood method to remove a link set member called from set/removeValue to
	 * ensure referential integrity for the GPO references.
	 * 
	 * @param property
	 * @param resource
	 */
	private void removeLinkSetMember(final URI property, final Resource resource) {
		
		materialize();
		
		final GPOEntry entry = establishLinkEntry(property);
		entry.remove(this, resource);
	}
	
	/**
	 * The under the hood method to add a link set member called from setValue to
	 * ensure referential integrity for the GPO references.
	 * 
	 * @param property
	 * @param resource
	 */
	private void addLinkSetMember(final URI property, final Resource resource) {
		
		materialize();
		
		final GPOEntry entry = establishLinkEntry(property);
		entry.add(this, resource);
	}
	
	@Override
	public void removeValue(final URI property, final Value value) {

		if (property == null)
			throw new IllegalArgumentException();

		if (value == null)
			throw new IllegalArgumentException();

//		final Lock writeLock = m_lock.writeLock();
//		writeLock.lock();
//		try {
			materialize();

			final GPOEntry entry = establishEntry(property);

			if (entry.remove(this, value)) {
				
				if (value instanceof Resource) {
					((GPO) m_om.getGPO((Resource) value)).removeLinkSetMember(property, m_id);
				}

				dirty();

			}
//		} finally {
//			writeLock.unlock();
//		}

	}

	@Override
	public void removeValues(final URI property) {

		if (property == null)
			throw new IllegalArgumentException();

//		final Lock writeLock = m_lock.writeLock();
//		writeLock.lock();
//		try {
			materialize();

			final GPOEntry entry = establishEntry(property);
			
			if (entry.m_size != 0) {
				clearReverseLinkSetEntries(entry);

				entry.removeAll(this);

				dirty();

			}
//		} finally {
//			writeLock.unlock();
//		}

	}
	
	private void clearReverseLinkSetEntries(final GPOEntry entry) {
		Iterator<Value> values = entry.values();
		while (values.hasNext()) {
			Value value = values.next();
			if (value instanceof Resource) {
				((GPO) m_om.getGPO((Resource) value)).removeLinkSetMember(entry.m_key, m_id);
			}
		}

	}

	/**
	 * If this {@link GPO} was clean, then mark the {@link GPO} as dirty and add
	 * it to the object manager's dirty list. This is a NOP if the {@link GPO}
	 * is already marked as dirty.
	 */
	private void dirty() {

		if (m_clean) {

			m_clean = false;

			((ObjectMgrModel) m_om).addToDirtyList(this);

		}

	}

	public boolean isDirty() {

		return !m_clean;

	}

	@Override
	public IGPO asGeneric() {
		return this;
	}

	@Override
	public String pp() {
//		final Lock readLock = m_lock.readLock();
//		readLock.lock();
//		try {
//			materialize();

			final StringBuilder out = new StringBuilder("ID: "
					+ m_id.stringValue() + "\n");

			GPOEntry entry = m_headEntry;
			while (entry != null) {
				Iterator<Value> values = entry.values();
				while (values.hasNext())
					out.append(entry.m_key.toString() + ": "
							+ values.next().toString() + "\n");

				entry = entry.m_next;
			}

			return out.toString();
//		} finally {
//			readLock.unlock();
//		}
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
	 * Called by the ObjectManager when flushing dirty objects. This can occur
	 * incrementally or on ObjectManager commit. The object is marked as clean
	 * once written.
	 */
	public void doCommit() {

		assert m_materialized;

		GPOEntry entry = m_headEntry;

		while (entry != null) {

			entry.commit();

			entry = entry.m_next;

		}

		m_clean = true;

	}

	// private void update(final GPOEntry entry) {
	//
	// assert m_materialized;
	//		
	// // final Iterator<Value> removes = entry.removes();
	// // while (removes.hasNext()) {
	// // m_om.retract(m_id, entry.m_key, removes.next());
	// // }
	// //
	// // final Iterator<Value> inserts = entry.additions();
	// // while (inserts.hasNext()) {
	// // m_om.insert(m_id, entry.m_key, inserts.next());
	// // }
	//		
	// entry.commit();
	// }

	/**
	 * Basis for lazy materialization, checks materialize state and if false
	 * requests matierialization from the ObjectManager
	 */
	public IGPO materialize() {
		checkLive();

		if (!m_materialized) {
            synchronized (this) {
                if (!m_materialized && m_stmt == null) {
					m_om.materialize(this);
					m_materialized = true;
				}
			}
		}
		return this;
	}

	public void setMaterialized(boolean b) {
		m_materialized = true;
	}

	// public void prepareBatchTerms() {
	// final ObjectMgrModel oom = (ObjectMgrModel) m_om;
	// GPOEntry entry = m_headEntry;
	// while (entry != null) {
	// final Iterator<Value> inserts = entry.additions();
	// while (inserts.hasNext()) {
	// final Value v = inserts.next();
	// oom.checkValue(v);
	// }
	//			
	// entry = entry.m_next;
	// }
	// }

	/**
	 * Adds statements to be inserted and removed to the appropriate lists based
	 * on the {@link GPO}s internal edit set.
	 * 
	 * @param insertList
	 *            The list of statements to be added.
	 * @param removeList
	 *            The list of statements to be removed.
	 */
	public void prepareBatchUpdate(final List<Statement> insertList,
			final List<Statement> removeList) {
//		final Lock readLock = m_lock.readLock();
//		readLock.lock();
//		try {
			final ObjectMgrModel oom = (ObjectMgrModel) m_om;
			final ValueFactory f = oom.getValueFactory();
			GPOEntry entry = m_headEntry;
			while (entry != null) {
				final Iterator<Value> inserts = entry.additions();
				while (inserts.hasNext()) {
					final Value v = inserts.next();
					final Statement stmt = f.createStatement(m_id, entry.m_key,
							v);
					insertList.add(stmt);
				}
				final Iterator<Value> removes = entry.removes();
				while (removes.hasNext()) {
					final Value v = removes.next();
					final Statement stmt = f.createStatement(m_id, entry.m_key,
							v);
					removeList.add(stmt);
				}
				entry = entry.m_next;
			}
//		} finally {
//			readLock.unlock();
//		}
	}

	/**
	 * The getSkin method is inspired somewhat by the Microsoft Win32
	 * getInterface that allowed an object to return multiple interfaces. The
	 * difference with the GPO skin is that the skin should be able to interact
	 * with any underlying GPO object.
	 * 
	 * <p>
	 * It may be worthwhile performance wise to cache a skin. I believe of more
	 * importance is to preserve identity - even of the interface/skin object.
	 * 
	 * @param skin
	 *            interface required
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
//		final Lock readLock = m_lock.readLock();
//		readLock.lock();
//		try {
			materialize();

			if (m_headEntry == null) {
				return new EmptyIterator<URI>();
			}

			ArrayList<URI> properties = new ArrayList<URI>();
			GPOEntry entry = m_headEntry;
			while (entry != null) {
				properties.add(entry.m_key);
				entry = entry.m_next;
			}

			return properties.iterator();
//		} finally {
//			readLock.unlock();
//		}
	}
}
