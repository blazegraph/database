/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 6, 2008
 */

package com.bigdata.rdf.store;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;

import com.sun.org.apache.xerces.internal.util.URI;

/**
 * An interface that defines bit masks that are applied to the low bits of the
 * assigned term identifiers and which indicate directly (without consulting the
 * term index) whether a term is a URI, Literal, BNode, or Statement. This is
 * used to quickly test the type of a term identifier without requiring the term
 * value to be materialized from the id:term index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITermIdCodes {

    /**
     * The bit mask that is bit-wise ANDed with a term identifier in order to
     * reveal the term code (the low two bits of the mask are set).
     * 
     * @see #TERMID_CODE_URI
     * @see #TERMID_CODE_BNODE
     * @see #TERMID_CODE_LITERAL
     * @see #TERMID_CODE_STATEMENT
     */
    static final public long TERMID_CODE_MASK = 0x03L;
    
    /**
     * The bit value used to indicate that a term identifier stands for a
     * {@link URI}.
     * <p>
     * Note: The lower two bits of a term identifier are reserved to indicate
     * the type of thing for which the term identifier stands {URI, Literal,
     * BNode, or Statement}. This is used to avoid lookup of the term in the
     * {@link #name_idTerm} index when we only need to determine the term class.
     */
    static final public long TERMID_CODE_URI = 0x00L;

    /**
     * The bit value used to indicate that a term identifier stands for a
     * {@link BNode}.
     */
    static final public long TERMID_CODE_BNODE = 0x01L;

    /**
     * The bit value used to indicate that a term identifier stands for a
     * {@link Literal}.
     */
    static final public long TERMID_CODE_LITERAL = 0x02L;

    /**
     * The bit value used to indicate that a term identifier stands for a
     * statement (when support for statement identifiers is enabled).
     */
    static final public long TERMID_CODE_STATEMENT = 0x03L;
    
}
