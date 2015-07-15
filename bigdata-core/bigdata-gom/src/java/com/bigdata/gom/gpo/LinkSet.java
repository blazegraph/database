package com.bigdata.gom.gpo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;

import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.ObjectMgrModel;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A (forward or reverse) link set.
 * 
 * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn
 *         Cutcher</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         FIXME We should materialize the link set unless the cardinality is
 *         excessive. This applies in both the forward and reverse direction. We
 *         will need a means to obtain a "DESCRIBE" of a resource (complete set
 *         of attributes, forward, and reverse links) and a "SKETCH" of a
 *         resource (cardinality counts only when a forward or reverse link set
 *         is large and otherwise they are inlined as well into the sketch).
 */
public class LinkSet implements ILinkSet {
	
    /**
     * The {@link IGPO} that is the head of the link set.
     */
    private final IGPO m_owner;
    /**
     * The predicate that collects this link set.
     */
	private final URI m_linkProperty;
	/**
	 * <code>true</code> iff this is a reverse link set.
	 */
	private final boolean m_linksIn;

    /**
     * 
     * @param owner
     *            The {@link IGPO} that is the head of the link set.
     * @param linkProperty
     *            The predicate that collects this link set.
     * @param linksIn
     *            <code>true</code> iff this is a reverse link set.
     */
    public LinkSet(final IGPO owner, final URI linkProperty,
            final boolean linksIn) {
        if (owner == null)
            throw new IllegalArgumentException();
        if (linkProperty == null)
            throw new IllegalArgumentException();
		m_owner = owner;
		m_linkProperty = linkProperty;
		m_linksIn = linksIn;
	}
	
	@Override
	public URI getLinkProperty() {
		return m_linkProperty;
	}

	@Override
	public IGPO getOwner() {
		return m_owner;
	}

	@Override
	public boolean isLinkSetIn() {
		return m_linksIn;
	}

	@Override
	public <C> Iterator<C> iterator(Class<C> theClassOrInterface) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int size() {
		final GPO.GPOEntry entry;
		if (m_linksIn) {
			entry = ((GPO) m_owner).getLinkEntry(m_linkProperty);
		} else {
			entry = ((GPO) m_owner).getEntry(m_linkProperty);
		}
		return entry == null ? 0 : entry.size();
	}

	@Override
	public long sizeLong() {
		return size();
	}

	@Override
	public boolean add(IGPO arg) {
		if (arg == null) {
			throw new IllegalArgumentException();
		}
		
		if (m_linksIn) {
			arg.addValue(m_linkProperty, m_owner.getId());
		} else {
			m_owner.addValue(m_linkProperty, arg.getId());
		}
		
		return true;
	}

	@Override
	public boolean addAll(Collection<? extends IGPO> arg) {
		if (arg == null) {
			throw new IllegalArgumentException();
		}
		for (IGPO mem : arg)
			add(mem);
		
		return true;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean contains(Object arg) {
		if (!(arg instanceof IGPO)) {
			throw new IllegalArgumentException("IGPO required");
		}
		IGPO gpo = (IGPO) arg;
		
		// since linkSet is fully materialized, just run through it
		Iterator<IGPO> values = iterator();
		while (values.hasNext()) {
			if (values.next() == gpo)
				return true;
		}
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * @see #size()
	 */
	@Override
	public boolean isEmpty() {
        // Not sufficient to check for null GPOEntry - items could have been removed
        return size() == 0;
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

        return ((GPO) m_owner).encode(v);

    }

    @Override
	public Iterator<IGPO> iterator() {
		if (false) {
            /*
             * Links in is not materialized.
             * 
             * TODO It should be fully materialized when the cardinality is not
             * excessive.
             */
			final IObjectManager om = m_owner.getObjectManager();
			
            final String query = "SELECT ?x\n" //
                    + "WHERE {\n"//
                    + "  ?x <"+ encode(m_linkProperty) + "> <" + encode(m_owner.getId())+ ">\n"//
                    +"}";//
			final ICloseableIterator<BindingSet> res = om.evaluate(query);
			
			return new Iterator<IGPO>() {
	
				@Override
				public boolean hasNext() {
					return res != null && res.hasNext();
				}
	
				@Override
				public IGPO next() {
					final BindingSet bs = res.next();
					
					return om.getGPO((Resource) bs.getBinding("x").getValue());
				}
	
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
				
			};
		} else {
		    /*
		     * Both Links in AND Links out are fully materialized on the gpo with DESCRIBE.
		     */
			final GPO.GPOEntry entry;
			if (m_linksIn) {
				entry = ((GPO) m_owner).getLinkEntry(m_linkProperty);
			} else {
				entry = ((GPO) m_owner).getEntry(m_linkProperty);
			}
			
			if (entry == null) {
				return new EmptyIterator<IGPO>();
			}
			
			return new Iterator<IGPO>() {
			    final Iterator<Value> m_values = entry.values();
				IGPO nextGPO = nextGPO();
				
				private IGPO nextGPO() {
					while (m_values.hasNext()) {
						final Value val = m_values.next();
						if (val instanceof Resource) {
							return m_owner.getObjectManager().getGPO((Resource) val);
						}
					}
					return null;
				}
				@Override
				public boolean hasNext() {
					return nextGPO != null;
				}
	
				@Override
				public IGPO next() {
					if (nextGPO == null)
						throw new NoSuchElementException();
					
					final IGPO ret = nextGPO;
					nextGPO = nextGPO();
					
					return ret;
				}
	
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
				
			};
		}
	}

	@Override
	public boolean remove(Object obj) {
		if (!(obj instanceof IGPO)) {
			throw new IllegalArgumentException("Expected an instance of IGPO");
		}
		
		final IGPO gpo = (IGPO) obj;
		
		final boolean ret;
		if (m_linksIn) {
			gpo.removeValue(m_linkProperty, m_owner.getId());
		} else {
			m_owner.removeValue(m_linkProperty, gpo.getId());
		}
		
		return true;
	}

	@Override
	public boolean removeAll(Collection<?> arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * Eagerly streams materialized objects into an array
	 */
	@Override
	public Object[] toArray() {
		final ArrayList<Object> out = new ArrayList<Object>();
		
		final Iterator<IGPO> gpos = iterator();
		while (gpos.hasNext()) {
			out.add(gpos.next());
		}
		
		return out.toArray();
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <C> Iterator<C> statements() {
		// TODO Auto-generated method stub
		return null;
	}

}
