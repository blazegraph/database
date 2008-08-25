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
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * An implementation using {@link BigdataValue}s and {@link BigdataStatement}s.
 * Values constructed using this factory do NOT have term identifiers assigned.
 * Statements constructed using this factory do NOT have statement identifiers
 * assigned. Those metadata can be resolved against the various indices and then
 * set on the returned values and statements.
 * 
 * @todo Hide {@link BigdataValue} ctors that can cause people problems. The
 *       public ctors are evil since they do not specify a value factory which
 *       causes a new {@link BigdataValue} be allocated once you try to write on
 *       the DB.
 * 
 * @todo Consider a {@link WeakValueCache} on this factory to avoid duplicate
 *       values.
 * 
 * @todo Consider a {@link WeakValueCache} to shortcut recently used statements?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataValueFactoryImpl implements ValueFactory, BigdataValueFactory {

    /**
     * @see IRawTripleStore#NULL
     */
    protected final long NULL = IRawTripleStore.NULL;

    /**
     * WARNING: Use {@link #getInstance(String)} NOT this constructor.
     */
    private BigdataValueFactoryImpl() {
        
    }
    
    /**
     * Canonicalizing mapping for {@link BigdataValueFactoryImpl}s based on the
     * namespace of the {@link LexiconRelation}.
     * <p>
     * Note: The backing LRU is small (it could be zero if that were allowed)
     * since instances SHOULD be finalized quickly once they are no longer
     * strongly reachable (which would imply that there was no
     * {@link LexiconRelation} for that instance and that all
     * {@link BigdataValueImpl}s for that instance had become weakly reachable
     * or been swept).
     */
    private static WeakValueCache<String/* namespace */, BigdataValueFactoryImpl> cache = new WeakValueCache<String, BigdataValueFactoryImpl>(
            new LRUCache<String, BigdataValueFactoryImpl>(1/* capacity */));

    /**
     * Return the instance associated with the <i>namespace</i>.
     * <p>
     * Note: This canonicalizing mapping for {@link BigdataValueFactoryImpl}s is
     * based on the namespace of the {@link LexiconRelation}. This makes the
     * instances canonical within a JVM instance, which is all that we care
     * about. The actual assignments of term identifiers to {@link BigdataValue}s
     * is performed by the {@link LexiconRelation} itself and is globally
     * consistent for a given lexicon.
     * 
     * @param namespace
     *            The namespace of the {@link LexiconRelation}.
     */
    public static BigdataValueFactoryImpl getInstance(String namespace) {
        
        if (namespace == null)
            throw new IllegalArgumentException();
        
        synchronized(cache) {
            
            BigdataValueFactoryImpl a = cache.get(namespace);

            if (a == null) {

                a = new BigdataValueFactoryImpl();

                cache.put(namespace, a, true/* dirty */);
                
            }
            
            return a;
            
        }
        
    }
    
    /**
     * Remove a {@link BigdataValueFactoryImpl} from the canonicalizing mapping.
     * <p>
     * Entries in this canonicalizing mapping for a {@link LexiconRelation} MUST
     * be {@link #remove(String)}ed if the {@link LexiconRelation} is destroyed
     * in case a distinct lexicon is subsequently creating with the same
     * namespace. There is no need to discard an entry during abort processing.
     * 
     * @param namespace
     *            The namespace of the {@link LexiconRelation}.
     */
    public static void remove(String namespace) {
        
        if (namespace == null)
            throw new IllegalArgumentException();
        
        synchronized(cache) {
        
            cache.remove(namespace);
            
        }
        
    }

    public BNodeContextFactory newBNodeContext() {

        return new BNodeContextFactory(this);
        
    }
    
    /**
     * Returns a new blank node with a globally unique blank node ID based on a
     * {@link UUID}.
     * <p>
     * Note: Since the blank node IDs are random, they tend to be uniformly
     * distributed across the index partition(s). More efficient ordered writes
     * may be realized using {@link #newBNodeContext()} to obtain a derived
     * {@link BigdataValueFactory} instance that is specific to a document that
     * is being loaded into the RDF DB. 
     * 
     * @see #newBNodeContext()
     */
    public BigdataBNodeImpl createBNode() {

        return createBNode(nextID());

    }

    /**
     * Returns a blank node identifier (ID) based on a random {@link UUID}.
     */
    protected String nextID() {

        return "_"+UUID.randomUUID();

    }
    
    public BigdataBNodeImpl createBNode(String id) {

        return new BigdataBNodeImpl(this, id);

    }

    public BigdataLiteralImpl createLiteral(String label) {

        return new BigdataLiteralImpl(this, label, null, null);
        
    }

    /*
     * XSD support. 
     */
    private static final transient String NAMESPACE_XSD = "http://www.w3.org/2001/XMLSchema";
    
    private static final transient String xsd = NAMESPACE_XSD + "#";

    private final BigdataURIImpl xsd_long = new BigdataURIImpl(this, xsd
            + "long");

    private final BigdataURIImpl xsd_int = new BigdataURIImpl(this,
            xsd + "int");

    private final BigdataURIImpl xsd_byte = new BigdataURIImpl(this, xsd
            + "byte");

    private final BigdataURIImpl xsd_short = new BigdataURIImpl(this, xsd
            + "short");

    private final BigdataURIImpl xsd_double = new BigdataURIImpl(this, xsd
            + "double");

    private final BigdataURIImpl xsd_float = new BigdataURIImpl(this, xsd
            + "float");

    private final BigdataURIImpl xsd_boolean = new BigdataURIImpl(this, xsd
            + "boolean");

    private final BigdataLiteralImpl TRUE = new BigdataLiteralImpl(this, "true", null,
            xsd_boolean);

    private final BigdataLiteralImpl FALSE = new BigdataLiteralImpl(this, "false", null,
            xsd_boolean);

    public BigdataLiteralImpl createLiteral(boolean arg0) {

        return (arg0 ? TRUE : FALSE);

    }

    public BigdataLiteralImpl createLiteral(byte arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_byte);

    }

    public BigdataLiteralImpl createLiteral(short arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_short);

    }

    public BigdataLiteralImpl createLiteral(int arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_int);

    }

    public BigdataLiteralImpl createLiteral(long arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_long);

    }

    public BigdataLiteralImpl createLiteral(float arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_float);

    }

    public BigdataLiteralImpl createLiteral(double arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_double);

    }

    public BigdataLiteralImpl createLiteral(XMLGregorianCalendar arg0) {

        return new BigdataLiteralImpl(this, arg0.toString(),
                null/* languageCode */, createURI(arg0.getXMLSchemaType()
                        .toString()));
    }

    public BigdataLiteralImpl createLiteral(String label, String language) {

        return new BigdataLiteralImpl(this, label, language, null/* datatype */);

    }

    public BigdataLiteralImpl createLiteral(String label, URI datatype) {

        if (!(datatype instanceof BigdataURIImpl)) {

            datatype = createURI(datatype.stringValue());

        }

        return new BigdataLiteralImpl(this, label, null,
                (BigdataURIImpl) datatype);

    }

    public BigdataURIImpl createURI(String uriString) {

        return new BigdataURIImpl(this, uriString);

    }

    public BigdataURIImpl createURI(String namespace, String localName) {

        return new BigdataURIImpl(this, namespace + localName);

    }

    public BigdataStatementImpl createStatement(Resource s, URI p, Value o) {

        return createStatement(s, p, o, null/* c */, null/* type */);

    }

    public BigdataStatementImpl createStatement(Resource s, URI p, Value o,
            Resource c) {

        return createStatement(s, p, o, c, null/* type */);

    }

    public BigdataStatementImpl createStatement(Resource s, URI p, Value o,
            Resource c, StatementEnum type) {

        return new BigdataStatementImpl(//
                (BigdataResource) asValue(s),//
                (BigdataURI)      asValue(p),//
                (BigdataValue)    asValue(o),//
                (BigdataResource) asValue(c),// optional
                type // the statement type (optional).
        );

    }

    final public BigdataValue asValue(Value v) {

        if (v == null)
            return null;

        if (v instanceof BigdataValueImpl
                && ((BigdataValueImpl) v).getValueFactory() == this) {

            // a value from the same value factory.
            return (BigdataValue) v;

        }

        if (v instanceof URI) {

            return createURI(((URI) v).stringValue());

        } else if (v instanceof BNode) {

            return createBNode(((BNode) v).stringValue());

        } else if (v instanceof Literal) {

            final Literal tmp = ((Literal) v);

            final String label = tmp.getLabel();

            final String language = tmp.getLanguage();

            final URI datatype = tmp.getDatatype();

            return new BigdataLiteralImpl(//
                    this,// Note: Passing in this factory!
                    label,//
                    language,//
                    (BigdataURI)asValue(datatype)//
                    );

        } else {

            throw new AssertionError();

        }

    }
    
    /**
     * (De-)serializer paired with this {@link BigdataValueFactoryImpl}.
     */
    private final transient BigdataValueSerializer<BigdataValue> valueSer = new BigdataValueSerializer<BigdataValue>(
            this);

    public BigdataValueSerializer<BigdataValue> getValueSerializer() {

        return valueSer;

    }

    public BigdataResource asValue(Resource v) {

        return (BigdataResource) asValue((Value) v);
        
    }

    public BigdataURI asValue(URI v) {
        
        return (BigdataURI)asValue((Value)v);
        
    }

    public BigdataLiteral asValue(Literal v) {
        
        return (BigdataLiteral)asValue((Value)v);
        
    }

    public BigdataBNode asValue(BNode v) {

        return (BigdataBNode)asValue((Value)v);
        
    }
    
}
