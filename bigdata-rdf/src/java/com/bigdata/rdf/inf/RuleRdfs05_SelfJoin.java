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
 * rdfs5: this variant assumes that it is reading from a single source (the
 * database) and does an in-memory self-join - it is NOT safe for use in truth
 * maintenance since it will fail to read from the focusStore.
 * 
 * <pre>
 *        triple(?u,rdfs:subPropertyOf,?x) :-
 *           triple(?u,rdfs:subPropertyOf,?v),
 *           triple(?v,rdfs:subPropertyOf,?x). 
 * </pre>
 */
public class RuleRdfs05_SelfJoin extends AbstractRuleRdfs_5_11 {

    public RuleRdfs05_SelfJoin(InferenceEngine inf) {

        super( inf.rdfsSubPropertyOf );

    }
    
}
