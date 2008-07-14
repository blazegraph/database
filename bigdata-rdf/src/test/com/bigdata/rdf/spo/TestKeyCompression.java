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
 * Created on Aug 22, 2007
 */

package com.bigdata.rdf.spo;

import junit.framework.TestCase;

/**
 * Test suite for approaches to key compression for statement indices (keys are
 * permutations on SPOC, logically comprised of long[4] and encoded as byte[]),
 * the terms index (key is byte[] encoding the URI, literal, or bnode ID), or
 * the ids index (key is byte[] encoding a long term identifier). Key
 * compression can be used (a) before sorting the data; (b) when serializing the
 * data for a remote operation on a data service; and (c) in the nodes and
 * leaves of the indices themselves.
 * <p>
 * 
 * @todo allow the client to use custom serialization for the keys and values.
 *       For example, for RDF statements inserted in sorted order a run length
 *       encoding by position would be very fast and compact:
 * 
 * <pre>
 *   [x][c][a]
 *   [x][d][b]
 *   [x][d][e]
 *   [x][f][a]
 *   
 *   would be about a 50% savings.
 *   
 *    x  c  a
 *    -  d  b
 *    -  -  e
 *    -  f  a
 *   
 *   Or
 *   
 *    4x 1c a 2d b e f a
 * </pre>
 * 
 * Since we never store identical triples, the last position always varies and
 * does not need a run length counter.
 * 
 * If we use a dictionary, then we can assign codes to term identifiers and
 * write out a code stream.
 * 
 * These two approaches can also be combined....
 * 
 * FIXME this was just a sketch of some ideas and this form of compression was
 * never implemented. There are some compression methods for RDF that have been
 * implemented and they SHOULD be tested here.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKeyCompression extends TestCase {

    /**
     * 
     */
    public TestKeyCompression() {
        super();
    }

    /**
     * @param arg0
     */
    public TestKeyCompression(String arg0) {
        super(arg0);
    }

    /**
     * @todo write tests.
     */
    public void test_nothing() {
        
    }

}
