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

/**
 * rdfs9:
 * <pre>
 *       triple(?v,rdf:type,?x) :-
 *          triple(?u,rdfs:subClassOf,?x),
 *          triple(?v,rdf:type,?u). 
 * </pre>
 */
public class RuleRdfs09 extends AbstractRuleRdfs_2_3_7_9 {

    public RuleRdfs09( InferenceEngine inf) {

        super( new Triple(var("v"), inf.rdfType, var("x")),//
                new Pred[] {//
                    new Triple(var("u"), inf.rdfsSubClassOf, var("x")),//
                    new Triple(var("v"), inf.rdfType, var("u"))//
                },
                new IConstraint[] {
                    new NE(var("u"),var("x"))
                });
    }

}
