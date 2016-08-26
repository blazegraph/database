/*

 Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

 Contact:
 SYSTAP, LLC DBA Blazegraph
 2501 Calvert ST NW #106
 Washington, DC 20008
 licenses@blazegraph.com

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

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.BooleanLiteralImpl;

import com.bigdata.cache.WeakValueCache;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedByteIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedIntIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedShortIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.util.concurrent.CanonicalFactory;

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
 */
public class BigdataValueFactoryImpl implements BigdataValueFactory {

	private final String namespace;
	
	@Override
	public String getNamespace() {
		
	    if (namespace != null) {
	        
	        return namespace;
	        
	    } else {
	        
	        throw new RuntimeException("Headless value factory should not be asked for its namespace");
	        
	    }
		
	}
	
    /**
     * WARNING: Use {@link #getInstance(String)} NOT this constructor.
     * <p>
     * WARNING: This constructor provides 'headless' (not associated with any
     * namespace) instance of the {@link BigdataValueFactory}, which is used for
     * query/update parsing. It SHOULD NOT be used in code working with
     * triple-store.
     * 
     * @see BLZG-1678 (remove "headless" BigdataValueFactory impl class)
     * @see BLZG-1176 (SPARQL Query/Update parser should not use db connection)
     */
    public BigdataValueFactoryImpl() {

        this(null);

    }

	/**
     * WARNING: Use {@link #getInstance(String)} NOT this constructor.
     */
    private BigdataValueFactoryImpl(final String namespace) {

        this.namespace = namespace;

        xsdMap = getXSDMap();
        
        // @see <a href="http://trac.blazegraph.com/ticket/983"> Concurrent insert data with boolean object causes IllegalArgumentException </a>
        // @see <a href="http://trac.blazegraph.com/ticket/980"> Object position query hint is not a Literal </a>
//        /**
//         * Cache the IV on the BigdataValue for these boolean constants.
//         * 
//         * @see <a href="http://trac.blazegraph.com/ticket/983"> Concurrent insert
//         *      data with boolean object causes IllegalArgumentException </a>
//         */
//        TRUE.setIV(XSDBooleanIV.TRUE);
//        FALSE.setIV(XSDBooleanIV.FALSE);
    }
    
	/**
	 * Canonicalizing mapping for {@link BigdataValueFactoryImpl}s based on the
	 * namespace of the {@link LexiconRelation}.
	 * <p>
	 * Note: The backing LRU should be small (and can be zero) since instances
	 * SHOULD be finalized quickly once they are no longer strongly reachable
	 * (which would imply that there was no {@link LexiconRelation} for that
	 * instance and that all {@link BigdataValueImpl}s for that instance had
	 * become weakly reachable or been swept).
	 */
    private static CanonicalFactory<String/* namespace */, BigdataValueFactoryImpl,String/*State*/> cache = new CanonicalFactory<String, BigdataValueFactoryImpl,String>(
            1/* capacity */) {
				@Override
				protected BigdataValueFactoryImpl newInstance(
						final String key, final String namespace) {
						return new BigdataValueFactoryImpl(namespace);
				}
    };
//    private static WeakValueCache<String/* namespace */, BigdataValueFactoryImpl> cache = new WeakValueCache<String, BigdataValueFactoryImpl>(
//            new LRUCache<String, BigdataValueFactoryImpl>(1/* capacity */));

    /**
     * Return the instance associated with the <i>namespace</i>.
     * <p>
     * Note: This canonicalizing mapping for {@link BigdataValueFactoryImpl}s is
     * based on the namespace of the {@link LexiconRelation}. This makes the
     * instances canonical within a JVM instance, which is all that we care
     * about. The actual assignments of term identifiers to {@link BigdataValue}
     * s is performed by the {@link LexiconRelation} itself and is globally
     * consistent for a given lexicon.
     * 
     * @param namespace
     *            The namespace of the {@link LexiconRelation}.
     * 
     *            TODO This method introduces the possibility that two journals
     *            in the same JVM would share the same
     *            {@link BigdataValueFactory} for a kb with the same namespace.
     *            This is doubtless not desired.  A workaround is to use the
     *            {@link UUID} of the Journal as part of the namespace of the
     *            KB, which would serve to make sure that all KB instances have
     *            distinct namespaces.
     */
	public static BigdataValueFactory/* Impl */getInstance(final String namespace) {

		return cache.getInstance(namespace, namespace/*state*/);
		
	}
	
//    /**
//     * Return the instance associated with the <i>namespace</i>.
//     * <p>
//     * Note: This canonicalizing mapping for {@link BigdataValueFactoryImpl}s is
//     * based on the namespace of the {@link LexiconRelation}. This makes the
//     * instances canonical within a JVM instance, which is all that we care
//     * about. The actual assignments of term identifiers to {@link BigdataValue}s
//     * is performed by the {@link LexiconRelation} itself and is globally
//     * consistent for a given lexicon.
//     * 
//     * @param namespace
//     *            The namespace of the {@link LexiconRelation}.
//     */
//    public static BigdataValueFactory/*Impl*/ getInstance(final String namespace) {
//        
//        if (namespace == null)
//            throw new IllegalArgumentException();
//        
//        synchronized(cache) {
//            
//            BigdataValueFactoryImpl a = cache.get(namespace);
//
//            if (a == null) {
//
//                a = new BigdataValueFactoryImpl();
//
//                cache.put(namespace, a, true/* dirty */);
//                
//            }
//            
//            return a;
//            
//        }
//        
//    }
    
    /**
     * Remove a {@link BigdataValueFactoryImpl} from the canonicalizing mapping.
     * <p>
     * Entries in this canonicalizing mapping for a {@link LexiconRelation} MUST
     * be {@link #remove(String)}ed if the {@link LexiconRelation} is destroyed
     * in case a distinct lexicon is subsequently creating with the same
     * namespace. There is no need to discard an entry during abort processing.
     * 
     */
//    * @param namespace
//    *            The namespace of the {@link LexiconRelation}.
    @Override
    public void remove(/*final String namespace*/) {
        
//        if (namespace == null)
//            throw new IllegalArgumentException();
//        
//        synchronized(cache) {
//        
//            cache.remove(namespace);
//            
//        }

    	cache.remove(namespace);
    	
    }

    @Override
    public String toString() {
        return super.toString()+"{namespace="+namespace+"}";
    }
    
    @Override
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
    @Override
    public BigdataBNodeImpl createBNode() {
        
        return createBNode(nextID());

    }

    /**
     * Returns a blank node identifier (ID) based on a random {@link UUID}.
     */
    protected String nextID() {

        return "u"+UUID.randomUUID();

    }
    
    @Override
    public BigdataBNodeImpl createBNode(final String id) {

        return new BigdataBNodeImpl(this, id);

    }

    @Override
    public BigdataBNodeImpl createBNode(final BigdataStatement stmt) {

    	// Subject, predicate, object and context should be processed to use the target value factory
    	// See https://jira.blazegraph.com/browse/BLZG-1875
    	final BigdataResource originalS = stmt.getSubject();
    	final BigdataURI originalP = stmt.getPredicate();
    	final BigdataValue originalO = stmt.getObject();
    	final BigdataResource originalC = stmt.getContext();
    	
    	final BigdataResource s = asValue(originalS);
    	final BigdataURI p = asValue(originalP);
    	final BigdataValue o = asValue(originalO);
    	final BigdataResource c = asValue(originalC);

    	final BigdataStatement effectiveStmt;
    	
		if (originalS != s || originalP != p || originalO != o || originalC != c) {

    		effectiveStmt = new BigdataStatementImpl(s, p, o, c, stmt.getStatementType(), stmt.getUserFlag());

    	} else {

    		effectiveStmt = stmt;
    		
    	}

		return new BigdataBNodeImpl(this, nextID(), effectiveStmt);
    }

    @Override
    public BigdataLiteralImpl createLiteral(final String label) {

        return new BigdataLiteralImpl(this, label, null, null);
        
    }

    /*
     * XSD support. 
     */
    public static final transient String NAMESPACE_XSD = "http://www.w3.org/2001/XMLSchema";
    
    public static final transient String xsd = NAMESPACE_XSD + "#";

    private final BigdataURIImpl xsd_string = new BigdataURIImpl(this, xsd
            + "string");

    private final BigdataURIImpl xsd_dateTime = new BigdataURIImpl(this,
            xsd + "dateTime");
    
    private final BigdataURIImpl xsd_date = new BigdataURIImpl(this,
            xsd + "date");
    
    private final BigdataURIImpl xsd_long = new BigdataURIImpl(this, xsd
            + "long");

    private final BigdataURIImpl xsd_int = new BigdataURIImpl(this,
            xsd + "int");

    private final BigdataURIImpl xsd_byte = new BigdataURIImpl(this, xsd
            + "byte");

    private final BigdataURIImpl xsd_short = new BigdataURIImpl(this, xsd
            + "short");

    private final BigdataURIImpl xsd_ulong = new BigdataURIImpl(this, xsd
            + "unsignedLong");

    private final BigdataURIImpl xsd_uint = new BigdataURIImpl(this,
            xsd + "unsignedInt");

    private final BigdataURIImpl xsd_ubyte = new BigdataURIImpl(this, xsd
            + "unsignedByte");

    private final BigdataURIImpl xsd_ushort = new BigdataURIImpl(this, xsd
            + "unsignedShort");

    private final BigdataURIImpl xsd_double = new BigdataURIImpl(this, xsd
            + "double");

    private final BigdataURIImpl xsd_float = new BigdataURIImpl(this, xsd
            + "float");

    private final BigdataURIImpl xsd_boolean = new BigdataURIImpl(this, xsd
            + "boolean");

//    private final BigdataLiteralImpl TRUE = new BigdataLiteralImpl(this, "true", null,
//            xsd_boolean);
//
//    private final BigdataLiteralImpl FALSE = new BigdataLiteralImpl(this, "false", null,
//            xsd_boolean);

	/**
	 * Map for fast resolution of XSD URIs. The keys are the string values of
	 * the URIs. The values are the URIs.
	 */
    private final Map<String,BigdataURIImpl> xsdMap;

    /**
     * Populate and return a map for fast resolution of XSD URIs.
     */
	private Map<String, BigdataURIImpl> getXSDMap() {

		final Map<String, BigdataURIImpl> map = new LinkedHashMap<String, BigdataURIImpl>();

		final BigdataURIImpl[] a = new BigdataURIImpl[] { xsd_string,
				xsd_dateTime, xsd_date, xsd_long, xsd_int, xsd_byte, xsd_short,
				xsd_double, xsd_float, xsd_boolean };

		for (BigdataURIImpl x : a) {

			// stringValue of URI => URI
			map.put(x.stringValue(), x);

		}

		return map;

    }
    
    /**
     * {@inheritDoc}
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/983"> Concurrent insert data
     *      with boolean object causes IllegalArgumentException </a>
     * @see <a href="http://trac.blazegraph.com/ticket/980"> Object position of
     *      query hint is not a Literal </a>
     */
    @Override
    public BigdataLiteralImpl createLiteral(final boolean arg0) {

        return (arg0 //
                ? new BigdataLiteralImpl(this, "true", null, xsd_boolean)
                : new BigdataLiteralImpl(this, "false", null, xsd_boolean)
                );

    }

    @Override
    public BigdataLiteralImpl createLiteral(byte arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_byte);

    }

    @Override
    public BigdataLiteralImpl createLiteral(byte arg0, final boolean unsigned) {

        return new BigdataLiteralImpl(this, "" + (unsigned ? XSDUnsignedByteIV.promote(arg0) : arg0), null, unsigned ? xsd_ubyte : xsd_byte);

    }

    @Override
    public BigdataLiteralImpl createLiteral(short arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_short);

    }

    @Override
    public BigdataLiteralImpl createLiteral(short arg0, final boolean unsigned) {

        return new BigdataLiteralImpl(this, "" + (unsigned ? XSDUnsignedShortIV.promote(arg0) : arg0), null, unsigned ? xsd_ushort :xsd_short);

    }

    @Override
    public BigdataLiteralImpl createLiteral(int arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_int);

    }

    @Override
    public BigdataLiteralImpl createLiteral(int arg0, final boolean unsigned) {

        return new BigdataLiteralImpl(this, "" +  (unsigned ? XSDUnsignedIntIV.promote(arg0) : arg0), null, unsigned ? xsd_uint :xsd_int);

    }

    @Override
    public BigdataLiteralImpl createLiteral(long arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_long);

    }

    @Override
    public BigdataLiteralImpl createLiteral(long arg0, final boolean unsigned) {

        return new BigdataLiteralImpl(this, "" + (unsigned ? XSDUnsignedLongIV.promote(arg0) : arg0), null, unsigned ? xsd_ulong : xsd_long);

    }

    @Override
    public BigdataLiteralImpl createLiteral(float arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_float);

    }

    @Override
    public BigdataLiteralImpl createLiteral(double arg0) {

        return new BigdataLiteralImpl(this, "" + arg0, null, xsd_double);

    }
    
    public BigdataLiteralImpl createLiteral(final Date date) {
        GregorianCalendar c = new GregorianCalendar();
        c.setTime(date);
        XMLGregorianCalendar xmlGC = 
                DateTimeExtension.datatypeFactorySingleton.newXMLGregorianCalendar(c);
        return createLiteral(xmlGC);
    }

    @Override
    public BigdataLiteralImpl createLiteral(final XMLGregorianCalendar arg0) {

		/*
		 * Note: QName#toString() does not produce the right representation,
		 * which is why we need to go through XMLDatatypeUtil.
		 * 
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/117
		 */
        return new BigdataLiteralImpl(this, arg0.toString(),
                null/* languageCode */, createURI(XMLDatatypeUtil.qnameToURI(
                        arg0.getXMLSchemaType()).stringValue()));
        
    }

    @Override
    public BigdataLiteralImpl createXSDDateTime(final long timestamp) {
        final TimeZone tz = TimeZone.getDefault()/*getTimeZone("GMT")*/;
        final GregorianCalendar c = new GregorianCalendar(tz);
        c.setGregorianChange(new Date(Long.MIN_VALUE));
        c.setTimeInMillis(timestamp);
        
        final XMLGregorianCalendar xmlGC = 
                DateTimeExtension.datatypeFactorySingleton.newXMLGregorianCalendar(c);
        return createLiteral(xmlGC);
    }


    @Override
    public BigdataLiteralImpl createLiteral(final String label, final String language) {

        return new BigdataLiteralImpl(this, label, language, null/* datatype */);

    }

    @Override
    public BigdataLiteralImpl createLiteral(final String label, URI datatype) {

        return createLiteral(label, datatype, null);
    }

    @Override
    public BigdataLiteralImpl createLiteral(String label, URI datatype, String language) {
        

        /*
         * Note: The datatype parameter may be null per the Sesame API.
         * 
         * See https://sourceforge.net/apps/trac/bigdata/ticket/226
         */
        if (datatype != null && !(datatype instanceof BigdataURIImpl)) {

            datatype = createURI(datatype.stringValue());
            
        }

        return new BigdataLiteralImpl(this, label, language,
                (BigdataURIImpl) datatype);
    }


    @Override
    public BigdataURIImpl createURI(final String uriString) {

		final String str = uriString;
		
//		if (str.startsWith(NAMESPACE_XSD)) {

			final BigdataURIImpl tmp = xsdMap.get(str);
			
			if(tmp != null) {

				// found in canonicalizing map.
				return tmp;
				
			}
    		
//    }
    
        return new BigdataURIImpl(this, uriString);

    }

    @Override
    public BigdataURIImpl createURI(final String namespace, final String localName) {

        return new BigdataURIImpl(this, namespace + localName);

    }

    @Override
    public BigdataStatementImpl createStatement(Resource s, URI p, Value o) {

        return createStatement(s, p, o, null/* c */, null/* type */);

    }

    @Override
    public BigdataStatementImpl createStatement(Resource s, URI p, Value o,
            Resource c) {

        return createStatement(s, p, o, c, null/* type */);

    }

    @Override
    public BigdataStatementImpl createStatement(Resource s, URI p, Value o,
            Resource c, StatementEnum type) {
        
        return createStatement(s, p, o, c, type, false/* userFlag */);
        
    }

    @Override
    public BigdataStatementImpl createStatement(Resource s, URI p, Value o,
            Resource c, StatementEnum type, final boolean userFlag) {
        
        return new BigdataStatementImpl(//
                (BigdataResource) asValue(s),//
                (BigdataURI)      asValue(p),//
                (BigdataValue)    asValue(o),//
                (BigdataResource) asValue(c),// optional
                type, // the statement type (optional).
                userFlag // the user flag (optional)
        );

    }

    @Override
    final public BigdataValue asValue(final Value v) {

        if (v == null)
            return null;

        if (v instanceof BigdataValueImpl
                && ((BigdataValueImpl) v).getValueFactory() == this) {

			final BigdataValueImpl v1 = (BigdataValueImpl) v;

			final IV<?, ?> iv = v1.getIV();

			if (iv == null || !iv.isNullIV()) {

				/*
				 * A value from the same value factory whose IV is either
				 * unknown or defined (but not a NullIV or DummyIV).
				 */

				return (BigdataValue) v;

			}

        }

        if (v instanceof BooleanLiteralImpl) {
        	
    		final BooleanLiteralImpl bl = (BooleanLiteralImpl) v;
    		
            return createLiteral(bl.booleanValue());

        } else if (v instanceof URI) {
        	
            return createURI(((URI) v).stringValue());
            
        } else if (v instanceof BigdataBNode && ((BigdataBNode)v).isStatementIdentifier()) {

       		return createBNode(((BigdataBNode) v).getStatement());

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

    @Override
    public BigdataValueSerializer<BigdataValue> getValueSerializer() {

        return valueSer;

    }

    @Override
    public BigdataResource asValue(Resource v) {

        return (BigdataResource) asValue((Value) v);
        
    }

    @Override
    public BigdataURI asValue(URI v) {
        
        return (BigdataURI)asValue((Value)v);
        
    }

    @Override
    public BigdataLiteral asValue(Literal v) {
        
        return (BigdataLiteral)asValue((Value)v);
        
    }

    @Override
    public BigdataBNode asValue(BNode v) {

        return (BigdataBNode)asValue((Value)v);
        
    }
    
}
