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
import com.bigdata.rdf.model.BigdataValue;

/**
 * Interface for decoding {@link IBindingSet}s comprised of {@link IV}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @see IBindingSetEncoder
 */
public interface IBindingSetDecoder {

    /**
     * Return <code>true</code> iff the {@link IVCache} associations are
     * preserved by the encoder.
     */
    boolean isValueCache();

    /**
     * Decode an {@link IBindingSet}.
     * <p>
     * The resolution step can be deferred when the decoded {@link IBindingSet}
     * does not require the resolved {@link IVCache} associations. For example,
     * we do not need the {@link IVCache} association in order to decide if two
     * {@link IBindingSet}s can join. However, once we have a solution from a
     * join, we may need to resolve the {@link IVCache} metadata for the joined
     * solution.
     * 
     * @param val
     *            The encoded IV[].
     * @param off
     *            The starting offset.
     * @param len
     *            The #of bytes of data to be decoded.
     * @param resolveCachedValues
     *            When <code>true</code>, any decoded {@link IV}s will have
     *            their {@link IVCache} association resolved before the
     *            {@link IBindingSet} is returned to the caller. When
     *            <code>false</code>, the resolution step is not performed.
     * 
     * @return The decoded {@link IBindingSet}.
     * 
     * @see #resolveCachedValues(IBindingSet)
     */
    IBindingSet decodeSolution(final byte[] val, final int off, final int len,
            final boolean resolveCachedValues);

    /**
     * Resolve any {@link IV}s in the solution for which there are cached
     * {@link BigdataValue}s to those values. This method may be used to resolve
     * {@link IVCache} associations for {@link IBindingSet}s NOT produced by
     * {@link #decodeSolution(byte[], int, int, boolean)}. For example, when
     * joining a decoded solution with another solution, the resolution step may
     * be deferred until we know whether or not the join was successful.
     * 
     * @param bset
     *            A solution having {@link IV}s which need to be reunited with
     *            their cached {@link BigdataValue}s.
     */
    void resolveCachedValues(final IBindingSet bset);

    /**
     * Release the state associated with the {@link IVBindingSetDecoder}.
     */
    void release();

}
