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
 * Created on Feb 16, 2012
 */

package com.bigdata.rdf.internal.encoder;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVCache;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Interface for encoding {@link IBindingSet}s comprised of {@link IV}s. The
 * interface is slightly more complicated than it otherwise would be due to (a)
 * {@link IVCache} and (b) the need to vector operations.
 * <p>
 * Encoding schemes tend to rely on {@link IVUtility} to provide fast and
 * efficient encode / decode. This is the same representation which is used in
 * the statement indices.
 * <p>
 * The {@link IVCache} interface may be used to associate a materialized
 * {@link BigdataValue} with an {@link IV}. In order to have efficient encode /
 * decode of {@link IV}s which preserves the {@link IVCache} information, the
 * encoding scheme also needs to store this association somewhere.
 * <p>
 * Encoders designed for the wire should store the {@link IVCache} association
 * in an inline representation. Encoders designed for main memory (for example,
 * vectored operations in hash joins) can use persistence capable data
 * structures to store the {@link IVCache} association.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @see IBindingSetDecoder
 */
public interface IBindingSetEncoder {

    /**
     * Return <code>true</code> iff the {@link IVCache} associations are
     * preserved by the encoder.
     */
    boolean isValueCache();
    
    /**
     * Encode the solution as an {@link IV}[], collecting updates for the
     * internal {@link IV} to {@link BigdataValue} cache.
     * 
     * @param bset
     *            The solution to be encoded.
     * 
     * @return The encoded solution.
     */
    byte[] encodeSolution(final IBindingSet bset);

    /**
     * Encode the solution as an {@link IV}[].
     * <p>
     * Note: The {@link IVCache} associations may be buffered by this method.
     * Use {@link #flush()} to vector any buffered associations.
     * 
     * @param bset
     *            The solution to be encoded.
     * @param updateCache
     *            When <code>true</code>, updates are accumulated for the
     *            {@link IV} to {@link BigdataValue} cache. You must still use
     *            {@link #flush()} to vector the accumulated updates.
     *            <p>
     *            If you are only generating the encoding in order to resolve a
     *            key in a hash index, then you would use <code>false</code>
     *            since you do not need to maintain the {@link IVCache}
     *            association for the given {@link IBindingSet}.
     * 
     * @return The encoded solution.
     */
    byte[] encodeSolution(final IBindingSet bset, final boolean updateCache);

    /**
     * Flush any updates. This allows for vectored operations when updating the
     * {@link IVCache} associations.
     */
    void flush();

    /**
     * Release the state associated with the {@link IVBindingSetEncoder}.
     */
    void release();

}
