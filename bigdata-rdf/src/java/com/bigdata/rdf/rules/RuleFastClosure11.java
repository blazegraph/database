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

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.bop.IConstraint;
import com.bigdata.bop.constraint.NE;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Rule for step 11 of {@link InferenceEngine#fastForwardClosure()}.
 * 
 * <pre>
 * (?x, rdf:type, ?b) :-
 *     (?x, ?y, ?z),
 *     (?y, rdfs:subPropertyOf, ?a),
 *     (?a, rdfs:domain, ?b).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleFastClosure11 extends AbstractRuleFastClosure_11_13 {

    /**
     * 
     */
    private static final long serialVersionUID = 7370736449328735275L;

    /**
     * @param vocab
     */
    public RuleFastClosure11(String relationName, Vocabulary vocab) {
        
        super(  "fastClosure11",//
                new SPOPredicate(relationName,var("x"), vocab.getConstant(RDF.TYPE), var("b")),//
                new SPOPredicate[] {//
                    new SPOPredicate(relationName,var("x"), var("y"), var("z")),//
                    new SPOPredicate(relationName,var("y"), vocab.getConstant(RDFS.SUBPROPERTYOF), var("a")),//
                    new SPOPredicate(relationName,var("a"), vocab.getConstant(RDFS.DOMAIN), var("b"))//
                },
                new IConstraint[] {
                    new NE(var("y"),var("a"))
        });
        
    }

}
