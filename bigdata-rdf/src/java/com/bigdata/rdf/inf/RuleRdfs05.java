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
 * rdfs5: this variant uses a nested subquery and may be safely used during
 * truth maintenance.
 * 
 * <pre>
 *        triple(?u,rdfs:subPropertyOf,?x) :-
 *           triple(?u,rdfs:subPropertyOf,?v),
 *           triple(?v,rdfs:subPropertyOf,?x). 
 * </pre>
 * 
 * @see RuleRdfs05_SelfJoin
 */
public class RuleRdfs05 extends AbstractRuleNestedSubquery {

    public RuleRdfs05(InferenceEngine inf) {

    super(  new Triple(var("u"), inf.rdfsSubPropertyOf, var("x")), //
            new Pred[] { //
                new Triple(var("u"), inf.rdfsSubPropertyOf, var("v")),//
                new Triple(var("v"), inf.rdfsSubPropertyOf, var("x")) //
            },
            new IConstraint[] {
                new NE(var("u"),var("v")),
                new NE(var("v"),var("x"))
            });

    }
    
}
