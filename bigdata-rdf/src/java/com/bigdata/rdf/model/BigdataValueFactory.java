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
 * Created on Apr 21, 2008
 */

package com.bigdata.rdf.model;

import java.util.UUID;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.rdf.store.IRawTripleStore;

/**
 * An implementation using {@link BigdataValue}s and {@link BigdataStatement}s.
 * Values constructed using this factory do NOT have term identifiers assigned.
 * Statements constructed using this factory do NOT have statement identifiers
 * assigned. Those metadata can be resolved against the various indices and then
 * set on the returned values and statements.
 * 
 * @todo consider a term cache on this factory to avoid duplicate values.
 * 
 * @todo explore refactor to use this exclusively in place of the
 *       {@link OptimizedValueFactory}. the main question is whether there is
 *       anything really "optimized" about the latter or if it is more a case of
 *       organic growth and the need for a refactor :-)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataValueFactory implements ValueFactory {

    /**
     * @see IRawTripleStore#NULL
     */
    protected final long NULL = IRawTripleStore.NULL;
    
    public BNode createBNode() {

        String id = "_"+UUID.randomUUID();

        return new BigdataBNodeImpl(id, NULL);
        
    }

    public BNode createBNode(String id) {
        
        return new BigdataBNodeImpl(id,NULL);
        
    }

    public Literal createLiteral(String label) {

        return new BigdataLiteralImpl(label,NULL);
        
    }

    /*
     * XSD support. 
     */
    private static final transient String NAMESPACE_XSD = "http://www.w3.org/2001/XMLSchema";
    
    private final String xsd = NAMESPACE_XSD+"#";

    private final BigdataURIImpl xsd_long = new BigdataURIImpl(xsd + "long",
            NULL);

    private final BigdataURIImpl xsd_int = new BigdataURIImpl(xsd + "int", NULL);

    private final BigdataURIImpl xsd_byte = new BigdataURIImpl(xsd + "byte",
            NULL);

    private final BigdataURIImpl xsd_short = new BigdataURIImpl(xsd + "short",
            NULL);

    private final BigdataURIImpl xsd_double = new BigdataURIImpl(
            xsd + "double", NULL);

    private final BigdataURIImpl xsd_float = new BigdataURIImpl(xsd + "float",
            NULL);

    private final BigdataURIImpl xsd_boolean = new BigdataURIImpl(xsd
            + "boolean", NULL);

    private final Literal TRUE = new BigdataLiteralImpl("true", xsd_boolean,
            NULL);

    private final Literal FALSE = new BigdataLiteralImpl("false", xsd_boolean,
            NULL);

    public Literal createLiteral(boolean arg0) {

        return (arg0 ? TRUE : FALSE);

    }

    public Literal createLiteral(byte arg0) {

        return new BigdataLiteralImpl("" + arg0, xsd_byte, NULL);

    }

    public Literal createLiteral(short arg0) {

        return new BigdataLiteralImpl("" + arg0, xsd_short, NULL);

    }

    public Literal createLiteral(int arg0) {

        return new BigdataLiteralImpl("" + arg0, xsd_int, NULL);

    }

    public Literal createLiteral(long arg0) {

        return new BigdataLiteralImpl("" + arg0, xsd_long, NULL);

    }

    public Literal createLiteral(float arg0) {

        return new BigdataLiteralImpl("" + arg0, xsd_float, NULL);

    }

    public Literal createLiteral(double arg0) {

        return new BigdataLiteralImpl("" + arg0, xsd_double, NULL);

    }

    public Literal createLiteral(XMLGregorianCalendar arg0) {
        
        return new BigdataLiteralImpl(arg0.toString(), new BigdataURIImpl(arg0
                .getXMLSchemaType().toString(), NULL), NULL);
    }

    public Literal createLiteral(String label, String language) {

        return new BigdataLiteralImpl(label, language, NULL);

    }

    public Literal createLiteral(String label, URI datatype) {

        if (!(datatype instanceof BigdataURIImpl)) {

            datatype = new BigdataURIImpl(datatype.stringValue(), NULL);

        }

        return new BigdataLiteralImpl(label, (BigdataURIImpl) datatype, NULL);

    }

    public URI createURI(String uriString) {

        return new BigdataURIImpl(uriString, NULL);

    }

    public URI createURI(String namespace, String localName) {

        return new BigdataURIImpl(namespace + localName, NULL);

    }

    /**
     * Create a statement whose {@link StatementEnum} is NOT specified.
     */
    public Statement createStatement(Resource s, URI p, Value o) {

        return new BigdataStatementImpl(//
                (BigdataResourceImpl)OptimizedValueFactory.INSTANCE.toSesameObject(s),//
                (BigdataURIImpl)OptimizedValueFactory.INSTANCE.toSesameObject(p),//
                (BigdataValueImpl)OptimizedValueFactory.INSTANCE.toSesameObject(o),//
                null // no statement type by default
                );
        
    }

    /**
     * Create a statement whose {@link StatementEnum} is NOT specified.
     */
    public Statement createStatement(Resource s, URI p, Value o, Resource c) {

        return new BigdataStatementImpl(//
                (BigdataResourceImpl)OptimizedValueFactory.INSTANCE.toSesameObject(s),//
                (BigdataURIImpl)OptimizedValueFactory.INSTANCE.toSesameObject(p),//
                (BigdataValueImpl)OptimizedValueFactory.INSTANCE.toSesameObject(o),//
                (BigdataResourceImpl)OptimizedValueFactory.INSTANCE.toSesameObject(c),//
                null // no statement type by default
                );
        
    }

}
