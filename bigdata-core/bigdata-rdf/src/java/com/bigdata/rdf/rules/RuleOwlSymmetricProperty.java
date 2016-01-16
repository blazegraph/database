/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Nov 1, 2007
 */

package com.bigdata.rdf.rules;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.IConstraint;
import com.bigdata.bop.constraint.Constraint;
import com.bigdata.bop.constraint.NE;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.Rule;

/**
 * owl:SymmetricProperty
 * 
 * <pre>
 *   (x rdf:type owl:SymmetricProperty), (a x b) -&gt; (b x a).
 * </pre>
 */
@SuppressWarnings("rawtypes")
public class RuleOwlSymmetricProperty extends Rule {

    /**
	 * 
	 */
	private static final long serialVersionUID = -6688762355076324400L;

	/**
     * @param vocab
     */
    public RuleOwlSymmetricProperty(String relationName, Vocabulary vocab) {

        super( "owlSymmetricProperty", //
                new SPOPredicate(relationName,var("b"), var("x"), var("a")),//
                new SPOPredicate[] {//
                    new SPOPredicate(relationName,var("x"), vocab.getConstant(RDF.TYPE), vocab.getConstant(OWL.SYMMETRICPROPERTY)),//
                    new SPOPredicate(relationName,var("a"), var("x"), var("b"))//
                },
                new IConstraint[] {
					Constraint.wrap(new NE(var("a"),var("b")))
		        }
                );
        
    }

}
