/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 29, 2007
 */

package com.bigdata.rdf.inf;

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

    public RuleRdfs04b(InferenceEngine inf) {

            super(inf.database, //
                    new Triple(var("v"), inf.rdfType, inf.rdfsResource), //
                    new Pred[] { //
                        new Triple(var("u"), var("a"), var("v"))//
                    });

        }


}
