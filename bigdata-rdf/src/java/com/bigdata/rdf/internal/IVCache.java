/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * materialized value is required for a downstream operator. The query plan will
 * {@link #dropValue() drop} the cached {@link Value} once it is no longer
 * required by the remaining operators in the plan.
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
public interface IVCache<V extends BigdataValue> {
    
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
     * Cache the materialized {@link Value}.
     * <p>
     * Note: This is normally invoked by {@link #asValue(LexiconRelation)}
     * during a lexicon join cache a newly materialized {@link Value} on the
     * {@link IV}.
     * 
     * @param val
     *            The {@link BigdataValue}.
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

    /**
     * Drop the cached {@link BigdataValue}. This is a NOP if the cache is
     * empty.
     */
    void dropValue();
    
    /**
     * Returns true if the RDF {@link BigdataValue} has been pre-materialized
     * and cached on this {@link IV}.
     */
    boolean hasValue();
    
}
