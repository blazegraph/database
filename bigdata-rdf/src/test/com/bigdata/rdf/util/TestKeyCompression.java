/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Aug 22, 2007
 */

package com.bigdata.rdf.util;

import junit.framework.TestCase;

/**
 * Test suite for approaches to key compression for statement indices (keys are
 * permutations on SPOC, logically comprised of long[4] and encoded as byte[]),
 * the terms index (key is byte[] encoding the URI, literal, or bnode ID), or
 * the ids index (key is byte[] encoding a long term identifier).  Key compression
 * can be used (a) before sorting the data; (b) when serializing the data for a
 * remote operation on a data service; and (c) in the nodes and leaves of the
 * indices themselves.
56 * <p>
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
 * These two approaches can also be combined.
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKeyCompression extends TestCase {

    /**
     * 
     */
    public TestKeyCompression() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @param arg0
     */
    public TestKeyCompression(String arg0) {
        super(arg0);
        // TODO Auto-generated constructor stub
    }

}
