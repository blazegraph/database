/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package cutthecrap.utils.striterators;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;



import cutthecrap.utils.striterators.IStriterator.ITailOp;

/**
 * Striterator - transformation and mapping patterns over java {@link Iterator}
 * s.
 * <p>
 * Allows wrapping of an iterator so that extensions may add type specific
 * next<Type> methods.
 * <p>
 * The {@link IFilter} objects passed to addFilter allow selection criteria for
 * the iterated objects. The <code>addTypeFilter</code> method allows easy
 * specification of a class type restriction.
 */
public class Striterator implements IStriterator, ITailOp, ICloseableIterator {

    private volatile List<IFilter> filters = null; // Note: NOT serializable.
    private volatile Iterator realSource;
	private volatile Iterator m_src = null;
	
	private boolean isOpen = true;

	/*
	 * package private method used by the unit tests.
	 */
	boolean isOpen() {
	    return isOpen;
	}
	
    /**
     * Deserialization constructor.
     */
    public Striterator() {
        this.realSource = null;
    }

    /** Constructor takes source iterator **/
	public Striterator(final Iterator src) {
	    this.realSource = src;
    }

    public Striterator(final Enumeration src) {
        this(new EnumIterator(src));
    }

    /**
     * 
     * @param src
     * @param filters
     */
    public Striterator(final Iterator src, final List<IFilter> filters) {
        this.realSource = src;
    }

    /** delegates hasNext request to source iterator **/
    @Override
    public boolean hasNext() {
    	if (!isOpen)
    		return false;
    	
        if (m_src == null)
            compile(realSource);
        
        boolean ret = m_src.hasNext();
        if (!ret) {
        	close();
        }
        return ret;
    }

    /** delegates next request to source iterator **/
    @Override
    public Object next() {
        if (m_src == null)
            compile(realSource);
        final Object ret =  m_src.next();
        // experimental tail optimisation
		if (m_src instanceof ITailOp) {
			Object old = m_src;
			m_src = ((ITailOp) m_src).availableTailOp();
		}
		return ret;
	}

	/** Enumeration version of hasNext() **/
    @Override
	public boolean hasMoreElements() {
		return hasNext();
	}

	/** Enumeration version of next() **/
    @Override
	public Object nextElement() {
		return next();
	}

	/** delegates remove request to source iterator **/
    @Override
	public void remove() {
		m_src.remove();
	}

	/** creates a Filterator to apply the filter **/
    @Override
	public IStriterator addFilter(final IFilter filter) {
        if (filters == null) {
            synchronized (this) {
                /*
                 * Note: double-checked locking pattern and volatile field are
                 * used to ensure visibility in combination with lazy create of
                 * the backing list.
                 */
                if (filters == null) {
                	filters = Collections
                            .synchronizedList(new LinkedList<IFilter>());
                }
            }
        }
        
        filters.add(filter);

        return this;
	}
	
    public void compile(final Iterator src) {
        compile(src, null/* context */);
    }

    public void compile(final Iterator src, final Object context) {
        if (m_src != null)
            throw new IllegalStateException();
        m_src = realSource = src;
        if (filters != null)
            for (IFilter filter : filters) {
                m_src = filter.filter(m_src, context);
            }
    }
	
	/** check each object against cls.isInstance(object) **/
    @Override
	public IStriterator addTypeFilter(final Class cls) {
        addFilter(new Filter() {
        public boolean isValid(Object obj) {
            boolean ret = cls.isInstance(obj);

  			return ret;
  		}
  	} );
  	
  	return this;
  }

	/** check each object against cls.isInstance(object) **/
    @Override
	public IStriterator addInstanceOfFilter(final Class cls) {
        addFilter(new Filter() {
        public boolean isValid(Object obj) {
            return obj == null ? false : obj.getClass() == obj;
        }
  	} );
  	
  	return this;
  }

	/** exclude the object from the iteration  **/
    @Override
	public IStriterator exclude(Object object) {
		return addFilter(new ExclusionFilter(object));
  }

	/** exclude the object from the iteration  **/
    @Override
	public IStriterator makeUnique() {
		return addFilter(new UniquenessFilter());
  }

	/** append the iteration  **/
    @Override
	public IStriterator append(Iterator iter) {
		return addFilter(new Appender(iter));
  }
	
	/** map the clients method against the Iteration, the Method MUST take a single Object valued parameter **/
    @Override
	public IStriterator map(Object client, Method method) {
		return addFilter(new Mapper(client, method));
	}

    /**
     * Human readable representation of the filter chain.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("{filterChain=" + filters);
        sb.append("}");
        return sb.toString();
    }

    /**
     * If this Striterator has not been overriden then return the
     * source iterator, or even better, try and recurse to the nested tailOp
     * if available.
     * 
     * This has been disabled since it appears to have a negative effect.
     * 
     * To reactivate, uncomment the ITailOp implementation declaration.
     * 
     * TODO: Investigate apparent performance degradation with activation
     * 
     * @return
     */
//    private boolean doneone = false;
	public Iterator availableTailOp() {
		final boolean avail =  Striterator.class == this.getClass();
		
		if (avail) {
			return (m_src instanceof ITailOp) ? ((ITailOp) m_src).availableTailOp() : m_src;
		} else {
			return this;
		}
	}

	/**
	 * The base close implementation ends the iteration with no other side-effects.
	 * 
	 * Users should override this method for any required side-effects but must also invoke
	 * this "super" method.
	 */
	@Override
	public void close() {
		if (isOpen && realSource instanceof ICloseable)
			((ICloseable) realSource).close();
		
		isOpen = false;
	}
}
