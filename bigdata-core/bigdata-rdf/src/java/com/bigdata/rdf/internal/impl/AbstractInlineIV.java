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
package com.bigdata.rdf.internal.impl;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Abstract base class for inline RDF values (literals, blank nodes, and
 * statement identifiers can be inlined).
 * <p>
 * {@inheritDoc}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
 *          thompsonbry $
 */
abstract public class AbstractInlineIV<V extends BigdataValue, T>
        extends AbstractIV<V, T> {

    /**
     * 
     */
    private static final long serialVersionUID = -2847844163772097836L;

    protected AbstractInlineIV(final VTE vte, final DTE dte) {

        super(vte, true/* inline */, false/* extension */, dte);

    }

    protected AbstractInlineIV(final VTE vte, final boolean extension,
            final DTE dte) {

        super(vte, true/*inline*/, extension, dte);

    }

    /**
     * Always returns <code>true</code> since the value is inline.
     */
    @Override
    final public boolean isInline() {
        return true;
    }

//	/**
//	 * Returns a human interpretable string value of the {@link IV} object. When
//	 * possible, returns either a Literal's label, a URI's URI or a BNode's ID.
//	 */
//    abstract public String stringValue();

//    /**
//     * No term identifier for an inline IV - throws an exception.
//     */
//    final public long getTermId() {
//        throw new UnsupportedOperationException();
//    }

    
}
