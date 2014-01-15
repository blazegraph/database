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

    public FilterBase() {
    }
    
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
////    	 makes most sense to consider the filterchain as preprocessing the
////    	 src prior to application of this filter.
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
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("{annotations=" + annotations);
        sb.append(",filterChain=" + filterChain);
        sb.append("}");
        return sb.toString();
    }

}
