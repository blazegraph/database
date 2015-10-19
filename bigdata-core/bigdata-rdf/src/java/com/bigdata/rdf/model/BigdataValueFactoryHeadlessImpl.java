/*

 Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

 Contact:
 SYSTAP, LLC
 2501 Calvert ST NW #106
 Washington, DC 20008
 licenses@systap.com

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
public class BigdataValueFactoryHeadlessImpl implements BigdataValueFactory {

	@Override
	public String getNamespace() {

		throw new RuntimeException("Should not happen");

}
	
    /**
     * WARNING: Use {@link #getInstance(String)} NOT this constructor.
     */
    public BigdataValueFactoryHeadlessImpl() {

        xsdMap = getXSDMap();

    }
    
    /**
     * Remove a {@link BigdataValueFactoryHeadlessImpl} from the canonicalizing mapping.
     * <p>
     * Entries in this canonicalizing mapping for a {@link LexiconRelation} MUST
     * be {@link #remove(String)}ed if the {@link LexiconRelation} is destroyed
     * in case a distinct lexicon is subsequently creating with the same
     * namespace. There is no need to discard an entry during abort processing.
     * 
     */
    @Override
    public void remove() {
        
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

        return new BigdataBNodeImpl(this, nextID(), stmt);

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

        /*
         * Note: The datatype parameter may be null per the Sesame API.
         * 
         * See https://sourceforge.net/apps/trac/bigdata/ticket/226
         */
        if (datatype != null && !(datatype instanceof BigdataURIImpl)) {

        	datatype = createURI(datatype.stringValue());
        	
        }

        return new BigdataLiteralImpl(this, label, null,
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
     * (De-)serializer paired with this {@link BigdataValueFactoryHeadlessImpl}.
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
