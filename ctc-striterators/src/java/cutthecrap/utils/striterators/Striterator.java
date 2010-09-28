/*
Striterator - transformation and mapping patterns over java Iterators

Copyright (C) SYSTAP, LLC 2010.  All rights reserved.

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

package cutthecrap.utils.striterators;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Striterator
 *
 * Allows wrapping of an iterator so that extensions may add type specific next<Type> methods.
 *
 * The IFilter objects passed to addFilter allow selection criteria for the iterated objects.
 *	The <code>addTypeFilter</code> method allows easy specification of a class type restriction.
 */
public class Striterator implements IStriterator {
	volatile List<IFilter> filters = null; // Note: NOT serializable.
    private transient Iterator realSource;
	private transient Iterator m_src = null;

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
     * @param filter
     */
    public Striterator(final Iterator src, final List<IFilter> filters) {
        this.realSource = src;
    }

    /** delegates hasNext request to source iterator **/
    public boolean hasNext() {
        if (m_src == null)
            compile(realSource);
        
        return m_src.hasNext();
    }

    /** delegates next request to source iterator **/
    public Object next() {
        if (m_src == null)
            compile(realSource);
        return m_src.next();
	}

	/** Enumeration version of hasNext() **/
	public boolean hasMoreElements() {
		return hasNext();
	}

	/** Enumeration version of next() **/
	public Object nextElement() {
		return next();
	}

	/** delegates remove request to source iterator **/
	public void remove() {
		m_src.remove();
	}

	/** creates a Filterator to apply the filter **/
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
	public IStriterator addTypeFilter(Class cls) {
		addFilter(new Filter(cls) {
  		protected boolean isValid(Object obj) {
  			boolean ret = ((Class) m_state).isInstance(obj);

  			return ret;
  		}
  	} );
  	
  	return this;
  }

	/** check each object against cls.isInstance(object) **/
	public IStriterator addInstanceOfFilter(Class cls) {
		addFilter(new Filter(cls) {
  		protected boolean isValid(Object obj) {
  			return obj.getClass() == m_state;
  		}
  	} );
  	
  	return this;
  }

	/** exclude the object from the iteration  **/
	public IStriterator exclude(Object object) {
		return addFilter(new ExclusionFilter(object));
  }

	/** exclude the object from the iteration  **/
	public IStriterator makeUnique() {
		return addFilter(new UniquenessFilter());
  }

	/** append the iteration  **/
	public IStriterator append(Iterator iter) {
		return addFilter(new Appender(iter));
  }
	
	/** map the clients method against the Iteration, the Method MUST take a single Object valued parameter **/
	public IStriterator map(Object client, Method method) {
		return addFilter(new Mapper(client, method));
	}

    /**
     * Human readable representation of the filter chain.
     */
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("{filterChain=" + filters);
        sb.append("}");
        return sb.toString();
    }

}
