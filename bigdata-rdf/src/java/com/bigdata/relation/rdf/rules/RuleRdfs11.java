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
package com.bigdata.relation.rdf.rules;

import com.bigdata.rdf.inf.RuleRdfs11_SelfJoin;
import com.bigdata.relation.IRelationName;
import com.bigdata.relation.rdf.SPO;
import com.bigdata.relation.rdf.SPOPredicate;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.NE;
import com.bigdata.relation.rule.Rule;

/**
 * rdfs11: this variant uses a nested subquery and may be safely used during
 * truth maintenance.
 * 
 * <pre>
 *       triple(?u,rdfs:subClassOf,?x) :-
 *          triple(?u,rdfs:subClassOf,?v),
 *          triple(?v,rdfs:subClassOf,?x). 
 * </pre>
 * 
 * @see RuleRdfs11_SelfJoin
 */
public class RuleRdfs11 extends Rule {

    /**
     * 
     */
    private static final long serialVersionUID = -8896684126297662485L;

    public RuleRdfs11(IRelationName<SPO> relationName, RDFSVocabulary inf) {

        super(  "rdfs11",//
                new SPOPredicate(relationName,var("u"), inf.rdfsSubClassOf, var("x")), //
                new SPOPredicate[] { //
                    new SPOPredicate(relationName,var("u"), inf.rdfsSubClassOf, var("v")),//
                    new SPOPredicate(relationName,var("v"), inf.rdfsSubClassOf, var("x")) //
                },
                new IConstraint[] {
                    new NE(var("u"),var("v")),
                    new NE(var("v"),var("x"))
                });

    }
    
}
