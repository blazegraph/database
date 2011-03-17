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
package com.bigdata.rdf.rules;

import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.bop.IConstraint;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.constraint.NE;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.vocab.Vocabulary;
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

    public RuleRdfs11(String relationName, Vocabulary vocab) {

        super(  "rdfs11",//
                new SPOPredicate(relationName,var("u"), vocab.getConstant(RDFS.SUBCLASSOF), var("x")), //
                new SPOPredicate[] { //
                    new SPOPredicate(relationName,var("u"), vocab.getConstant(RDFS.SUBCLASSOF), var("v")),//
                    new SPOPredicate(relationName,var("v"), vocab.getConstant(RDFS.SUBCLASSOF), var("x")) //
                },
                new IConstraint[] {
        			Constraint.wrap(new NE(var("u"),var("v"))),
    				Constraint.wrap(new NE(var("v"),var("x")))
                });

    }
    
}
