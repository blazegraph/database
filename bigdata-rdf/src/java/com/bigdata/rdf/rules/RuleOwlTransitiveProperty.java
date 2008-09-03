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
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.NE;
import com.bigdata.relation.rule.Rule;

/**
 * owl:TransitiveProperty
 * 
 * <pre>
 * (a rdf:type owl:TransitiveProperty), (x a y), (y a z) -&gt; (x a z)
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleOwlTransitiveProperty extends Rule
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param vocab
     */
    public RuleOwlTransitiveProperty(String relationName, Vocabulary vocab) {

        super(  "owlTransitiveProperty",//
                new SPOPredicate(relationName,var("x"), var("a"), var("z")), //
                new SPOPredicate[] { //
                    new SPOPredicate(relationName,var("a"), vocab.getConstant(RDF.TYPE), vocab.getConstant(OWL.TRANSITIVEPROPERTY)),//
                    new SPOPredicate(relationName,var("x"), var("a"), var("y")),//
                    new SPOPredicate(relationName,var("y"), var("a"), var("z"))//
                }, new IConstraint[] {
                    new NE(var("x"),var("y")),
                    new NE(var("y"),var("z")),
                    new NE(var("x"),var("z")),
                    }
                );
        
    }

}
