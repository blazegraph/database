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
import com.bigdata.striterator.ICloseableIterator;

import cutthecrap.utils.striterators.EmptyIterator;

public class LinkSet implements ILinkSet {
	final IGPO m_owner;
	final URI m_linkProperty;
	final boolean m_linksIn;
	
	public LinkSet(final IGPO owner, final URI linkProperty, final boolean linksIn) {
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
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long sizeLong() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean add(IGPO arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends IGPO> arg0) {
		// TODO Auto-generated method stub
		return false;
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
		return gpo.getValue(m_linkProperty) == m_owner;
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator<IGPO> iterator() {
		if (m_linksIn) {
			final IObjectManager om = m_owner.getObjectManager();
			
			final String query = "SELECT ?x WHERE {?x <" + m_linkProperty.toString() + "> <" + m_owner.getId().toString() + ">}";
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
			final GPO.GPOEntry entry = ((GPO) m_owner).getEntry(m_linkProperty);
			if (entry == null) {
				return new EmptyIterator<IGPO>();
			}
			
			return new Iterator<IGPO>() {
				Iterator<Value> m_values = entry.values();
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
			ret = gpo.getValue(m_linkProperty) == m_owner;
			if (ret) {
				gpo.setValue(m_linkProperty, null);
			}
		} else { // FIXME: implement linksOut
			throw new UnsupportedOperationException();
		}
		
		return ret;
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
