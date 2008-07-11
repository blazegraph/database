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
/*
 * Created on Nov 1, 2007
 */
package com.bigdata.rdf.rules;

import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.relation.IRelationIdentifier;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.Rule;

/**
 * owl:sameAs2
 * 
 * <pre>
 * (x owl:sameAs y), (x a z) -&gt; (y a z).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleOwlSameAs2 extends Rule  {

    /**
     * 
     */
    private static final long serialVersionUID = -8016648183305295727L;

    /**
     * @param vocab
     */
    public RuleOwlSameAs2(IRelationIdentifier<SPO> relationName, RDFSVocabulary vocab) {

        super(  "owlSameAs2",//
                new SPOPredicate(relationName,var("y"), var("a"), var("z")), //
                new SPOPredicate[] { //
                    new SPOPredicate(relationName,var("x"), vocab.owlSameAs, var("y")),//
                    new SPOPredicate(relationName,var("x"), var("a"), var("z"))//
                },
                new IConstraint[]{
                    /*
                     * Reject (y sameAs z) as the head.
                     */
                    new RejectAnythingSameAsItself(var("y"),var("a"),var("z"),vocab.owlSameAs)
//                    , new NEConstant(var("a"),inf.owlSameAs.id)
                }
        );
       
    }

}
