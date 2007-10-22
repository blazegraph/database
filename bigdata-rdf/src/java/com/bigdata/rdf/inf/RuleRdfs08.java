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
package com.bigdata.rdf.inf;

/**
 * @todo Should we run this rule or backchain?
 *       <p>
 *       [MikeP] I would say generate, but you can backchain (I think?) as long
 *       as you have the full closure of the type hierarchy, specifically of ?
 *       type Class. If you don't have that (i.e. if you are not calculating the
 *       domain and range inferences), then you'd need to recursively backchain
 *       the tail too. That is why I did not do more backchaining - the
 *       backchain for ? type Resource is super easy because it's never longer
 *       than 1 move back.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleRdfs08 extends AbstractRuleRdfs_6_8_10_12_13 {

    public RuleRdfs08(InferenceEngine inf, Var u, Var v, Var x) {

        super(inf, new Triple(u, inf.rdfsSubClassOf, inf.rdfsResource), 
                     new Triple(u, inf.rdfType, inf.rdfsClass));

    }
    
}