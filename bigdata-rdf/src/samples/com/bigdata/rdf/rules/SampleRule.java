/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.bop.IConstraint;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.constraint.NE;
import com.bigdata.bop.constraint.NEConstant;
import com.bigdata.rdf.internal.constraints.InferenceBVE;
import com.bigdata.rdf.internal.constraints.IsLiteralBOp;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.Rule;

/**
 * If A is similar to B and both are of the same real type (not rdfs:Resource),
 * then attach B's datatype properties (literals) to A as well. 
 * 
 * ?a similarTo ?b .
 * ?a rdf:type ?t .
 * ?b rdf:type ?t .
 * ?b ?p ?o .
 * filter(isLiteral(?o)) .
 * filter(?t != rdfs:Resource) .
 * filter(?a != ?b) .
 * ->
 * ?a ?p ?o 
 */
public class SampleRule extends Rule {
	
	private static final long serialVersionUID = 7627609187312677342L;

	
	public SampleRule(final String relationName, final Vocabulary vocab) {

        super(  "SampleRule", // rule name
                new SPOPredicate(relationName, var("a"), var("p"), var("o")), // head
                new SPOPredicate[] { // tail
                    new SPOPredicate(relationName, var("a"), vocab.getConstant(SAMPLE.SIMILAR_TO), var("b")),
                    new SPOPredicate(relationName, var("a"), vocab.getConstant(RDF.TYPE), var("t")),
                    new SPOPredicate(relationName, var("b"), vocab.getConstant(RDF.TYPE), var("t")),
                    new SPOPredicate(relationName, var("b"), var("p"), var("o")),
                },
                new IConstraint[] { // constraints
					Constraint.wrap(new NE(var("a"), var("b"))),
        			Constraint.wrap(new NEConstant(var("t"), vocab.getConstant(RDFS.RESOURCE))),
        			// you can use SPARQL value expression bops in inference by wrapping them with an InferenceBVE
                    Constraint.wrap(new InferenceBVE(new IsLiteralBOp(
                            var("o"))))
                });

    }

}
