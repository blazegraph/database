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

import org.openrdf.model.vocabulary.OWL;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.NE;
import com.bigdata.relation.rule.Rule;

/**
 * owl:inverseOf1
 * 
 * <pre>
 *   owl:inverseOf1 : (a owl:inverseOf b) -&gt; (b owl:inverseOf a)
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleOwlInverseOf1 extends Rule {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param vocab
     */
    public RuleOwlInverseOf1(String relationName, Vocabulary vocab) {

        super(  "owlInverseOf1",//
                new SPOPredicate(relationName,var("b"), vocab.getConstant(OWL.INVERSEOF), var("a")), //
                new SPOPredicate[] { //
                    new SPOPredicate(relationName,var("a"), vocab.getConstant(OWL.INVERSEOF), var("b")),//
                },
                new IConstraint[] {
                    new NE(var("a"),var("b"))
                }
                );
        
    }

}
