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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Used with Filterator by Striterator to filter returned objects. This can
 * represent either a single {@link IFilter} or a chain of {@link IFilter}s.
 */
public abstract class FilterBase implements IFilter, Cloneable {

    private static final long serialVersionUID = 1L;

    /**
     * An optional filter chain. When non-<code>null</code> this contains the
     * {@link IFilter} in the same order in which they were added to this
     * {@link IFilter}, but <i>this</i> is NOT included in the filter chain.
     * <p>
     * Note: package private for unit tests.
     */
    /*private*/ volatile List<IFilter> filterChain = null;

    /**
     * Annotations may be used to decorate the {@link IFilter} with required or
     * optional metadata.
     * <p>
     * Note: package private for unit tests.
     */
    /*private*/ volatile Map<String, Object> annotations;

    /**
     * State from the constructor (optional).
     * <p>
     * Note: Striterators should not have a side-effect on state object since
     * that can have unexpected consequences if the {@link IFilter} is reused.
     */
    final protected Object m_state;

    public FilterBase() {
        m_state = null;
    }
    
    /**
     * 
     * @param state
     *            State (optional).
     */
    public FilterBase(final Object state) {
        m_state = state;
    }

//    public FilterBase clone() {
//        
//        final FilterBase inst = new FilterBase(m_state);
//        
//        for(IFilter filter : filterChain) {
//            
//            inst.addFilter(filter);
//            
//        }
//        
//        return inst;
//        
//    }
    
    /**
     * Add a filter to the end of this filter chain.
     * 
     * @param filter
     *            The filter.
     *            
     * @return This filter.
     */
    final public FilterBase addFilter(final IFilter filter) {
        if (filter == null)
            throw new IllegalArgumentException();
        if (filterChain == null) {
            synchronized (this) {
                /*
                 * Note: double-checked locking pattern and volatile field are
                 * used to ensure visibility in combination with lazy create of
                 * the backing list.
                 */
                if (filterChain == null) {
                    filterChain = Collections
                            .synchronizedList(new LinkedList<IFilter>());
                }
            }
        }
        filterChain.add(filter);

        return this;
    }

    final public Iterator filter(Iterator src, final Object context) {
        // wrap src with _this_ filter.
        src = filterOnce(src, context);
        if (filterChain != null) {
            // wrap source with each additional filter from the filter chain.
            for (IFilter filter : filterChain) {
                src = filter.filter(src, context);
            }
        }
        return src;
    }

    /**
     * Wrap the source iterator with <i>this</i> filter.
     * 
     * @param src
     *            The source iterator.
     * @param context
     *            The iterator evaluation context.
     *            
     * @return The wrapped iterator.
     */
    abstract protected Iterator filterOnce(Iterator src, final Object context);

    final public Object getProperty(String name) {

        if (annotations == null)
            return null;
        
        return annotations.get(name);
        
    }

    /**
     * Return the value of a named property.
     * 
     * @param name
     *            The property name.
     * 
     * @return The property value.
     * 
     * @throws IllegalStateException
     *             unless the named property is bound.
     */
    final protected Object getRequiredProperty(final String name) {
        
        final Object value = getProperty(name);
        
        if (value == null)
            throw new IllegalStateException(name);
        
        return value;
        
    }
    
    /**
     * Set an annotation.
     * 
     * @param name
     *            The name.
     * @param value
     *            The value.
     *            
     * @return The old value.
     */
    final public Object setProperty(final String name, final Object value) {

        if (name == null)
            throw new IllegalArgumentException();
        
        if (annotations == null) {
            /*
             * Note: double-checked locking pattern and volatile field are used
             * to ensure visibility in combination with lazy create of the
             * annotations map.
             */
            synchronized (this) {
                if (annotations == null) {
                    annotations = Collections
                            .synchronizedMap(new LinkedHashMap<String, Object>());
                }
            }
        }
        return annotations.put(name, value);
    }

    /**
     * Human readable representation of the filter chain.
     */
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("{annotations=" + annotations);
        sb.append(",filterChain=" + filterChain);
        sb.append("}");
        return sb.toString();
    }

}
