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
 * Created on Oct 29, 2007
 */

package com.bigdata.rdf.rules;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * rdfs4b:
 * 
 * <pre>
 * (?u ?a ?v) -&gt; (?v rdf:type rdfs:Resource)
 * </pre>
 * 
 * Note: Literals can be entailed in the subject position by this rule and MUST
 * be explicitly filtered out. That task is handled by the
 * {@link DoNotAddFilter}. {@link RuleRdfs03} is the other way that literals
 * can be entailed into the subject position.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleRdfs04b extends AbstractRuleDistinctTermScan {

    /**
     * 
     */
    private static final long serialVersionUID = 5770961957356857919L;

    public RuleRdfs04b(String relationName,Vocabulary vocab) {

            super(  "rdfs04b",//
                    new SPOPredicate(relationName,var("v"), vocab.getConstant(RDF.TYPE), vocab.getConstant(RDFS.RESOURCE)), //
                    new SPOPredicate[] { //
                        new SPOPredicate(relationName,var("u"), var("a"), var("v"))//
                    },//
                    null//constraints
                    );

        }


}
