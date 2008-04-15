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
 * Created on Apr 12, 2008
 */

package com.bigdata.rdf.store;

/**
 * A vocabulary for bigdata specific extensions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface BNS {

    /**
     * The namespace used for bigdata specific extensions.
     */
    String NAMESPACE = "http://www.bigdata.com/rdf#";

    /**
     * The name of an attribute whose value is recognized in RDF/XML as the
     * statement identifier for statement described by the element on which it
     * appears. The <code>bigdata:sid</code> attribute is allowed on elements
     * describing RDF statements and serves as a per-statement identifier. The
     * value of the <code>bigdata:sid</code> attribute must conform to the
     * syntactic constraints of a blank node. By re-using the value of the
     * <code>bigdata:sid</code> attribute within other statements in the same
     * RDF/XML document, the client can express statements about the statements.
     * <p>
     * This RDF/XML extension allows us to inline provenance (statements about
     * statements) with RDF/XML to clients in a manner which has minor impact on
     * unaware clients (they will perceive additional statements whose predicate
     * is <code>bigdata:sid</code>). In addition, clients aware of this
     * extension can submit RDF/XML with inline provenance.
     * <p>
     * For example:
     * 
     * <pre>
     * &lt;rdf:Description rdf:about=&quot;http://www.foo.org/A&quot;&gt;
     *     &lt;label bigdata:sid=&quot;_S67&quot; xmlns=&quot;http://www.w3.org/2000/01/rdf-schema#&quot;&gt;abc&lt;/label&gt;
     * &lt;/rdf:Description&gt;
     * </pre>
     */
    String SID = "sid";
    
}
