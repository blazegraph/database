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
package com.bigdata.rdf.inf;

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

        this(db,head,body,null);
        
    }
    
    public AbstractRuleRdfs_2_3_7_9
    ( AbstractTripleStore db, 
      Triple head, 
      Pred[] body,
      IConstraint[] constraints
      ) {

        super(db, head, body, constraints);

        // only two predicates in the tail.
        assert body.length == 2;

        // only one shared variable.
        assert getSharedVars(0/* body[0] */, 1/* body[1] */).size() == 1;

    }
}
