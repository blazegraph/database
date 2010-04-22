/*

 Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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

import com.bigdata.cache.ConcurrentWeakValueCache;
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
 * @todo Consider a {@link WeakValueCache} on this factory to avoid duplicate
 *       values.
 * 
 * @todo Consider a {@link WeakValueCache} to shortcut recently used statements?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataValueFactoryImpl implements BigdataValueFactory {

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
     * 
     * @todo replace with {@link ConcurrentWeakValueCache} and a zero backing
     *       hard reference LRU capacity?
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
    public static BigdataValueFactoryImpl getInstance(final String namespace) {
        
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
    public static void remove(final String namespace) {
        
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
    
    public BigdataBNodeImpl createBNode(final String id) {

        return new BigdataBNodeImpl(this, id);

    }

    public BigdataLiteralImpl createLiteral(final String label) {

        return new BigdataLiteralImpl(this, label, null, null);
        
    }

    /*
     * XSD support. 
     */
    public static final transient String NAMESPACE_XSD = "http://www.w3.org/2001/XMLSchema";
    
    public static final transient String xsd = NAMESPACE_XSD + "#";

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

    public BigdataLiteralImpl createLiteral(final XMLGregorianCalendar arg0) {

        return new BigdataLiteralImpl(this, arg0.toString(),
                null/* languageCode */, createURI(arg0.getXMLSchemaType()
                        .toString()));
    }

    public BigdataLiteralImpl createLiteral(final String label, final String language) {

        return new BigdataLiteralImpl(this, label, language, null/* datatype */);

    }

    public BigdataLiteralImpl createLiteral(final String label, URI datatype) {

        if (!(datatype instanceof BigdataURIImpl)) {

            datatype = createURI(datatype.stringValue());

        }

        return new BigdataLiteralImpl(this, label, null,
                (BigdataURIImpl) datatype);

    }

    public BigdataURIImpl createURI(final String uriString) {

        return new BigdataURIImpl(this, uriString);

    }

    public BigdataURIImpl createURI(final String namespace, final String localName) {

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

    final public BigdataValue asValue(final Value v) {

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
    private final transient BigdataValueSerializer<BigdataValueImpl> valueSer = new BigdataValueSerializer<BigdataValueImpl>(
            this);

    public BigdataValueSerializer<BigdataValueImpl> getValueSerializer() {

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
