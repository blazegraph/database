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


/**
 * This interface defines the signed byte values indicating the type of a term
 * in the term index. The purpose of this is to partition the term:id index into
 * disjoint key ranges where the order of the terms in each of those partitions
 * is well defined. I.e., URIs are in one key range and the total order over the
 * URIs is well defined. The literals are broken down into a key range
 * corresponding to the plain literals, the language type literals, and the data
 * type literals (which is further broken down by the data type in order to
 * produce well-defined total orders).
 * <p>
 * Note: when these signed bytes get encoded as unsigned bytes in a key their
 * values change. For example, 2 becomes 130.
 * 
 * @see ITermIdCodes, defines bit masks that are applied to the low bits of the
 *      assigned term identifiers and which indicate directly (without
 *      consulting the term index) whether a term is a URI, Literal, BNode, or
 *      Statement.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITermIndexCodes {
    
    /** indicates a URI. */
    final public static byte TERM_CODE_URI = 0x01;

    /** indicates a plain literal. */
    final public static byte TERM_CODE_LIT = 0x02;

    /** indicates a literal with a language code. */
    final public static byte TERM_CODE_LCL = 0x03;

    /** indicates a literal with a data type URI. */
    final public static byte TERM_CODE_DTL = 0x04;

    /** indicates a blank node. */
    final public static byte TERM_CODE_BND = 0x05;
    
    /**
     * Indicates a statement identifier (used for statements about statements
     * where the statement identifier is used in any of the subject, predicate,
     * or object positions).
     * <p>
     * Note: Statement identifiers are assigned the highest code so that they
     * will be processed last when doing an ordered write of terms on the terms
     * index. This allows us to ensure that the component term identifiers in
     * the statement have been resolved before the statement itself is written
     * into the terms index.
     */
    final public static byte TERM_CODE_STMT = 0x06;

}
