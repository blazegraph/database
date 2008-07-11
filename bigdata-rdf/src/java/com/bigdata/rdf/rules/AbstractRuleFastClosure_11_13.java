/*

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
package com.bigdata.rdf.rules;

import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.Rule;

/**
 * Rule for steps 11 and 13 of the "fast closure" method.
 * <p>
 * Note: this rule is not very selective and does not produce new entailments
 * unless your ontology and your application both rely on domain/range to confer
 * type information. If you explicitly type your instances then this will not
 * add information during closure.
 * <p>
 * Step 11.
 * 
 * <pre>
 * (?x, rdf:type, ?b) :-
 *     (?x, ?y, ?z),
 *     (?y, rdfs:subPropertyOf, ?a),
 *     (?a, rdfs:domain, ?b).
 * </pre>
 * 
 * Step 13.
 * 
 * <pre>
 * (?z, rdf:type, ?b ) :-
 *       (?x, ?y, ?z),
 *       (?y, rdfs:subPropertyOf, ?a),
 *       (?a, rdfs:range, ?b ).
 * </pre>
 * 
 * @see TestRuleFastClosure_11_13
 */
abstract public class AbstractRuleFastClosure_11_13 extends Rule {

    protected final long propertyId;
    
    final IVariable<Long> x, y, z, a, b;
    final IConstant<Long> C1, C2;
    
    /**
     * 
     * @param head
     * @param body
     * @param constraints
     */
    public AbstractRuleFastClosure_11_13(String name, SPOPredicate head,
            SPOPredicate[] body, IConstraint[] constraints) {

        super(name, head, body, constraints);

        // validate the binding pattern for the tail of this rule.
        assert body.length == 3;

        // (x,y,z)
        x = (IVariable<Long>) body[0].s();
        y = (IVariable<Long>) body[0].p();
        z = (IVariable<Long>) body[0].o();

        // (y,C1,a)
        assert y == (IVariable<Long>) body[1].s();
        C1 = (IConstant<Long>) body[1].p();
        a = (IVariable<Long>) body[1].o();

        // (a,C2,b)
        assert a == (IVariable<Long>) body[2].s();
        C2 = (IConstant<Long>) body[2].p();
        b = (IVariable<Long>)body[2].o();

        this.propertyId = C2.get();
        
    }

}
