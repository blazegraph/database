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
 * rdfs7:
 * <pre>
 *       triple(?u,?b,?y) :-
 *          triple(?a,rdfs:subPropertyOf,?b),
 *          triple(?u,?a,?y).
 * </pre>
 */
public class RuleRdfs07 extends AbstractRuleRdfs_2_3_7_9 {

    public RuleRdfs07( RDFSHelper inf) {

        super( new Triple(var("u"), var("b"), var("y")),//
                new Pred[] {//
                    new Triple(var("a"), inf.rdfsSubPropertyOf, var("b")),//
                    new Triple(var("u"), var("a"), var("y"))//
                },
                new IConstraint[] {
                    new NE(var("a"),var("b"))
                }
        );

    }

}
