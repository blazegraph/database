/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jun 4, 2011
 */

package com.bigdata.rdf.internal;

import org.openrdf.model.Value;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Interface for managing the {@link BigdataValue} cached on an {@link IV}.
 * <p>
 * This interface is designed to support the query plan generator. The
 * {@link BigdataValue} is cached when a query plan decides that the
 * materialized value is required for a downstream operator.
 * <p>
 * Both {@link IV} and {@link BigdataValue} can cache one another. The pattern
 * for caching is that you <em>always</em> cache the {@link IV} on the
 * {@link BigdataValue} using {@link BigdataValue#setIV(IV)}. However, the
 * {@link BigdataValue} is normally NOT cached on the {@link IV}. The exception
 * is when the {@link BigdataValue} has been materialized from the {@link IV} by
 * joining against the lexicon.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IVCache<V extends BigdataValue,T> {
    
    /**
     * If the value is not already cached, then inflate an inline RDF value to a
     * {@link BigdataValue} and cache it on a private field.
     * <p>
     * Note: Query plans are responsible for ensuring that {@link IV}s have been
     * materialized before operators are evaluated which invoke this method.
     * This pattern ensures that efficient batch operators are used to
     * materialize {@link Value}s, and thereby avoids heavy RMI overhead in
     * scale-out, and provides operators which use {@link #getValue()} with a
     * simple method signature which does not require access to the lexicon.
     * Query plans are also responsible for dropping variables once they are no
     * longer needed or, in the case of large values and BLOBs, dropping the
     * cached {@link BigdataValue} when possible in order to avoid excess
     * network and heap overhead.
     * 
     * @param lex
     *            The lexicon relation (this is required in order to access the
     *            {@link BigdataValueFactory} for the namespace associated with
     *            lexicon when we materialize an inline {@link IV}).
     * 
     * @return The corresponding {@link BigdataValue}.
     * 
     * @throws UnsupportedOperationException
     *             if the {@link IV} does not represent something which can be
     *             materialized. For example, a dummy value or a "null".
     */
    V asValue(final LexiconRelation lex) 
        throws UnsupportedOperationException;

    /**
	 * Set the {@link BigdataValue} on the cache.
	 * <p>
	 * Note: This is normally invoked by {@link #asValue(LexiconRelation)}
	 * during a lexicon join cache a newly materialized {@link Value} on the
	 * {@link IV}.
	 * 
	 * @param val
	 *            The {@link BigdataValue}.
	 *            
	 * @return The argument.
	 */
    V setValue(final V val);
    
    /**
     * Return a pre-materialized RDF {@link BigdataValue} which has been cached
     * on this {@link IV} by a previous invocation of
     * {@link #asValue(LexiconRelation)}.
     * 
     * @return The {@link BigdataValue}.
     * 
     * @throws NotMaterializedException
     *             if the value is not cached.
     */
    V getValue() throws NotMaterializedException;

//	/**
//	 * Drop the cached {@link BigdataValue}. This is a NOP if the cache is
//	 * empty.
//	 * 
//	 * @deprecated There is a concurrency problem with this method for any IV for
//	 * which we are sharing the reference among multiple threads. That includes
//	 * the Vocabulary IVs and anything served out of the termCache. Probably the
//	 * method should be dropped. It was intended for us in scale-out and is not
//	 * currently invoked. Most of the time when we do not need the materialized
//	 * Value any longer, we will simply drop the variable. The exception is
//	 * BLOBs in scale-out. There we could replace the IV (if it was materialized
//	 * in advance of its last necessary usage) with an IV that has a blob
//	 * reference (or just send the blob reference rather than the blob).
//	 */
//    void dropValue();

    /**
     * Return a copy of this {@link IV}.
     * <p>
     * Note: This method exists to defeat the hard reference from the {@link IV}
     * to the cached {@link BigdataValue} in order to avoid a memory leak when
     * the {@link IV} is used as the key in a weak value cache whose value is
     * the {@link BigdataValue}. Therefore, certain {@link IV} implementations
     * MAY return <i>this</i> when they are used for limited collections. The
     * vocabulary IVs are the primary example. For the same reason, we do not
     * need to recursively break the link from the {@link IV} to the
     * {@link BigdataValue} for {@link IV}s which embed other {@link IV}s.
     * 
     * @param clearCache
     *            When <code>true</code> the cached reference (if any) will NOT
     *            be set on the copy.
     * 
     * @return The copy.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/437 (Thread-local
     *      cache combined with unbounded thread pools causes effective memory
     *      leak)
     */
    IV<V, T> clone(boolean clearCache);

    /**
     * Returns true if the RDF {@link BigdataValue} has been pre-materialized
     * and cached on this {@link IV}.
     */
    boolean hasValue();

}
