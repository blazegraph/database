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
 * Common base class for rdfs5 and rdfs11 using an in-memory self-join.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractRuleRdfs_5_11 extends AbstractRuleChainedSelfJoin {

    public AbstractRuleRdfs_5_11(Id C) {

        super( new Triple(var("u"), C, var("x")), //
                new Pred[] { //
                    new Triple(var("u"), C, var("v")),//
                    new Triple(var("v"), C, var("x")) //
                },
                new IConstraint[] {
                    new NE(var("u"),var("v")),
                    new NE(var("v"),var("x"))
                });
        
    }
        
}
