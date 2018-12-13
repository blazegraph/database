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
 * Created on Aug 14, 2008
 */

package com.bigdata.rdf.model;

import java.util.Date;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Interface strengthens the return types and adds some custom extensions.
 * 
 * @see BigdataValueFactoryImpl#getInstance(String)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface BigdataValueFactory extends ValueFactory {
	
    /**
     * The namespace of the KB instance associated with the value factory.
     */
    public String getNamespace();

    /**
	 * Remove instance of valueFactory from static cache
	 */
	void remove(/*final String namespace*/);
	
    /**
     * Returns a factory that will assign its blank node IDs within a globally
     * unique namespace. This factory should be used when processing a document
     * as the generated IDs are clustered and make the ordered writes on the
     * lexicon more efficient since all blank nodes for the same document tend to
     * be directed to the same index partition. All {@link BigdataValue}s are
     * actually created by <i>this</i> factory, it is only the semantics of
     * blank node ID generation that are overridden.
     * 
     * @see BNodeContextFactory
     */
    public BigdataValueFactory newBNodeContext();
    
//    /**
//     * Create a blank node and flag it as a statement identifier.
//     */
//    BigdataBNodeImpl createSID();
//    
//    /**
//     * Create a blank node with the specified ID and flag it as a statement
//     * identifier.
//     */
//    BigdataBNodeImpl createSID(String id);

    BigdataBNode createBNode();

    BigdataBNode createBNode(String id);
    
    BigdataBNode createBNode(BigdataStatement stmt);

    BigdataLiteral createLiteral(String label);

    BigdataLiteral createLiteral(boolean arg0);

    BigdataLiteral createLiteral(byte arg0);

    BigdataLiteral createLiteral(short arg0);

    BigdataLiteral createLiteral(int arg0);

    BigdataLiteral createLiteral(long arg0);

    BigdataLiteral createLiteral(byte arg0, boolean unsigned);

    BigdataLiteral createLiteral(short arg0, boolean unsigned);

    BigdataLiteral createLiteral(int arg0, boolean unsigned);

    BigdataLiteral createLiteral(long arg0, boolean unsigned);

    BigdataLiteral createLiteral(float arg0);

    BigdataLiteral createLiteral(double arg0);

    BigdataLiteral createLiteral(XMLGregorianCalendar arg0);

    BigdataLiteral createLiteral(Date arg0);

    BigdataLiteral createXSDDateTime(long timestamp);
        
    BigdataLiteral createLiteral(String label, String language);

    BigdataLiteral createLiteral(String label, URI datatype);

    BigdataLiteral createLiteral(String label, URI datatype, String language);

    BigdataURI createURI(String uriString);

    BigdataURI createURI(String namespace, String localName);

    /**
     * Create a statement whose {@link StatementEnum} is NOT specified.
     */
    BigdataStatement createStatement(Resource s, URI p, Value o);

    /**
     * Create a statement whose {@link StatementEnum} is NOT specified.
     */
    BigdataStatement createStatement(Resource s, URI p, Value o, Resource c);

    /**
     * Create a statement (core impl). The s,p,o, and the optional c arguments
     * will be normalized to this {@link BigdataValueFactory} using
     * {@link #asValue(Value)}.
     * 
     * @param s
     *            The subject.
     * @param p
     *            The predicate.
     * @param o
     *            The object.
     * @param c
     *            The context (optional). Note: When non-<code>null</code>
     *            and statement identifiers are enabled, then this will be a
     *            blank node whose term identifier is the statement identifier.
     * @param type
     *            The statement type (optional).
     */
    BigdataStatement createStatement(Resource s, URI p, Value o,
            Resource c, StatementEnum type);
    
    /**
     * Create a statement (core impl). The s,p,o, and the optional c arguments
     * will be normalized to this {@link BigdataValueFactory} using
     * {@link #asValue(Value)}.
     * 
     * @param s
     *            The subject.
     * @param p
     *            The predicate.
     * @param o
     *            The object.
     * @param c
     *            The context (optional). Note: When non-<code>null</code>
     *            and statement identifiers are enabled, then this will be a
     *            blank node whose term identifier is the statement identifier.
     * @param type
     *            The statement type (optional).
     * @param userFlag
     *            The user flag
     */
    BigdataStatement createStatement(Resource s, URI p, Value o,
            Resource c, StatementEnum type, boolean userFlag);

    /**
     * Converts a {@link Value} into a {@link BigdataValue}. If the value is
     * already a {@link BigdataValue} and it was allocated by <i>this</i>
     * {@link BigdataValueFactoryImpl} then it is returned unchanged. Otherwise a
     * new {@link BigdataValue} will be creating using the same data as the
     * given value and the term identifier on the new {@link BigdataValue} will
     * be initialized to {@link IRawTripleStore#NULL}.
     * <p>
     * All {@link BigdataValue}s created by a {@link BigdataValueFactoryImpl}
     * internally store a transient reference to the {@link BigdataValueFactoryImpl}.
     * This reference is used to decide if a {@link BigdataValue} MIGHT have
     * been created by a different lexicon (term identifiers generated by
     * different lexicons CAN NOT be used interchangeably). This has the effect
     * of protecting against incorrect use of the term identifier with a
     * database backed by a different lexicon while allowing reuse of the
     * {@link BigdataValue}s when possible.
     * 
     * @param v
     *            The value.
     * 
     * @return A {@link BigdataValue} with the same data. If the value is
     *         <code>null</code> then <code>null</code> is returned.
     */
    BigdataValue asValue(Value v);

    /**
     * Strongly typed for {@link Resource}s.
     */
    BigdataResource asValue(Resource v);
    
    /**
     * Strongly typed for {@link URI}s.
     */
    BigdataURI asValue(URI v);

    /**
     * Strongly typed for {@link Literal}s.
     */
    BigdataLiteral asValue(Literal v);

    /**
     * Strongly typed for {@link BNode}s.
     */
    BigdataBNode asValue(BNode v);

    /**
     * An object that can efficiently (de-)serialize {@link Value}s using this
     * {@link ValueFactory}. When the values are de-serialized they will have a
     * reference to this {@link BigdataValueFactoryImpl}.  That reference can be
     * used to identify when two {@link BigdataValue}s MIGHT be from different
     * lexicons.
     * 
     * @return
     */
    BigdataValueSerializer<BigdataValue> getValueSerializer();

    /**
     * Get this factory's implementation of rdf:langString
     */
    BigdataURI getLangStringURI();

    /**
     * Get this factory's implementation of xsd:string
     */
    BigdataURI getXSDStringURI();
}
