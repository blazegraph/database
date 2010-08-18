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
/*
 * Created on Jul 9, 2010
 */

package com.bigdata.rdf.internal;

import java.io.ObjectStreamException;

import com.bigdata.bop.Constant;
import com.bigdata.bop.Var;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * A dummy {@link IV} class used in cases where we want a dummy {@link Constant}
 * rather than an unbound {@link Var variable}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DummyIV extends AbstractIV {

    private static final long serialVersionUID = 1L;

    final public static transient IV INSTANCE = new DummyIV();
    
    private DummyIV() {
        super(VTE.BNODE, false/* inline */, false/* extension */, DTE.XSDBoolean);
    }

    public String toString() {
        return "DummyIV";
    }
    
    public BigdataValue asValue(final BigdataValueFactory f, 
            final ILexiconConfiguration config)
            throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    public Object getInlineValue() throws UnsupportedOperationException {
        return null;
    }

    /**
     * Note: This returns {@link TermId#NULL} for backwards compatibility.
     */
    public long getTermId() throws UnsupportedOperationException {
        return TermId.NULL;
    }

    public boolean isNull() {
        return false;
    }

    public int compareTo(Object o) {
        return 0;
    }

    protected int _compareTo(IV o) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return 27;
    }

    /**
     * Imposes the canonicalizing mapping during object de-serialization.
     */
    private Object readResolve() throws ObjectStreamException {
        
        return INSTANCE;
        
    }

    public int byteLength() {
        return 0;
    }

}
