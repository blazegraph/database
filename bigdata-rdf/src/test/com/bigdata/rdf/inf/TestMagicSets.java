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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.inf;

import java.io.IOException;

import junit.framework.TestCase2;

/**
 * Test suite for inference engine and the magic sets implementation.
 * <p>
 * The magic transform of the rule base {rdfs9, rdfs11} for the query
 * <code>triple(?s,rdf:type,A)?</code> is:
 * 
 * <pre>
 *    
 *    // by rule 1 of the magic transform.
 *    
 *    magic(triple(?s,rdf:type,A)). // the magic seed fact.
 *    
 *    // by rule 2 of the magic transform.
 *    
 *    triple(?v,rdf:type,?x) :-
 *       magic(triple(?u,rdfs:subClassOf,?x))
 *       triple(?u,rdfs:subClassOf,?x),
 *       triple(?v,rdf:type,?u). 
 *    
 *    triple(?u,rdfs:subClassOf,?x) :-
 *       magic(triple(?u,rdfs:subClassOf,?x))
 *       triple(?u,rdfs:subClassOf,?v),
 *       triple(?v,rdf:subClassOf,?x). 
 *    
 *    // by rule 3 of the magic transform ???
 *    
 * </pre>
 * 
 * The magic transform is:
 * <ol>
 * <li>add the query as a seed magic fact</li>
 * <li>insert a magic predicate as the first predicate in the body of each
 * rule. this magic predicate is formed by wrapping the head of the rule in a
 * "magic()" predicate.</li>
 * <li><em>it is not clear to me that rule3 applies for our rules</em></li>
 * </ol>
 * <p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMagicSets extends TestCase2 {

    /**
     * 
     */
    public TestMagicSets() {
    }

    /**
     * @param name
     */
    public TestMagicSets(String name) {
        super(name);
    }

    /**
     * Test of query answering using magic sets.
     *
     * @todo define micro kb based on mike's example and work through the
     * magic sets implementation.
     * 
     * @throws IOException
     */
    public void testQueryAnswering01() throws IOException {

        fail("implement test");
        
    }
    
}
