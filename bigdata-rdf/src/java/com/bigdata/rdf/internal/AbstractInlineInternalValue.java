/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
package com.bigdata.rdf.internal;

import org.openrdf.model.Value;

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
abstract public class AbstractInlineInternalValue<V extends BigdataValue, T>
        extends AbstractInternalValue<V, T> {

    /**
     * 
     */
    private static final long serialVersionUID = -2847844163772097836L;

    protected AbstractInlineInternalValue(final VTE vte,
            final DTE dte) {

        super(vte, true/* inline */, false/* extension */, dte);

    }
    
    /**
     * Returns the String-value of a Value object. This returns either a
     * Literal's label, a URI's URI or a BNode's ID.
     * 
     * @see Value#stringValue()
     */
    abstract public String stringValue();

    /**
     * Always returns <code>true</code> since the value is inline.
     */
    final public boolean isInline() {
        return true;
    }

    /**
     * Always returns <code>false</code> since the value is inline.
     */
    final public boolean isTermId() {
        return false;
    }

    /**
     * Always returns <code>false</code> since the value is inline.
     */
    final public boolean isNull() {
        return false;
    }

    public String toString() {
        
        return super.getDTE() + "(" + stringValue() + ")";
        
    }
    
}