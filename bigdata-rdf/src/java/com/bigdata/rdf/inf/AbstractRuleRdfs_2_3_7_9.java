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
package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * <p>
 * Abstract base class for rules with two terms in the tail where one term is
 * one-bound and the other is one bound (or two bound) by virtue of a join
 * variable.  The examples are:
 * </p>
 * <pre>
 * rdfs2: (u rdf:type x) :- (a rdfs:domain x),        (u a y).
 * 
 * rdfs3: (v rdf:type x) :- (a rdfs:range  x),        (u a v).
 * -----> (y rdf:type x) :- (a rdfs:range  x),        (u a y).
 * 
 * rdfs7: (u b        y) :- (a rdfs:subPropertyOf b), (u a y).
 * -----> (u x        y) :- (a rdfs:subPropertyOf x), (u a y).
 * </pre>
 * <p>
 * The second form for each rule above is merely rewritten to show that the
 * same variable binding patterns are used by each rule.
 * </p>
 * <p>
 * While the following rule has a different variable binding pattern it can
 * be evaluated by the same logic.
 * </p>
 * <pre>
 * rdfs9: (v rdf:type x) :- (u rdfs:subClassOf x),    (v rdf:type u).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractRuleRdfs_2_3_7_9 extends AbstractRuleNestedSubquery {

    public AbstractRuleRdfs_2_3_7_9
        ( AbstractTripleStore db, 
          Triple head, 
          Pred[] body
          ) {

        super( db, head, body );

        // only two predicates in the tail.
        assert body.length == 2;
        
        // only one shared variable.
        assert getSharedVars(0/*body[0]*/, 1/*body[1]*/).size() == 1;
        
    }
    
}
