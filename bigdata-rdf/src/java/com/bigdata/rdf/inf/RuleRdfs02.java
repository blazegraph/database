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
 * rdfs2:
 * <pre>
 * ( u rdf:type x) :- ( a rdfs:domain x), ( u a y ).
 * </pre>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleRdfs02 extends AbstractRuleRdfs_2_3_7_9 {

    public RuleRdfs02(InferenceEngine inf) {

        super(inf.database, new Triple(var("u"), inf.rdfType, var("x")),
                new Pred[] {
                new Triple(var("a"), inf.rdfsDomain, var("x")),
                new Triple(var("u"), var("a"), var("y"))
                });

    }

}
