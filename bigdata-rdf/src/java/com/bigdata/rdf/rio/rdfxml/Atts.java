/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.rdfxml;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A collection of XML attributes.
 * @openrdf
 */
class Atts {

	/*-----------*
	 * Variables *
	 *-----------*/

	/**
	 * List containing Att objects.
	 */
	private List<Att> attributes;

	/*--------------*
	 * Constructors *
	 *--------------*/

	/**
	 * Creates a new <tt>Atts</tt> object.
	 */
	public Atts() {
		this(4);
	}

	/**
	 * Creates a new <tt>Atts</tt> object.
	 * 
	 * @param size
	 *        The initial size of the array for storing attributes.
	 */
	public Atts(int size) {
		attributes = new ArrayList<Att>(size);
	}

	/*---------*
	 * Methods *
	 *---------*/

	/**
	 * Adds an attribute.
	 */
	public void addAtt(Att att) {
		attributes.add(att);
	}

	/**
	 * Get an iterator on the attributes.
	 * 
	 * @return an Iterator over Att objects.
	 */
	public Iterator<Att> iterator() {
		return attributes.iterator();
	}

	/**
	 * Gets the attribute with the specified QName.
	 * 
	 * @param qName
	 *        The QName of an attribute.
	 * @return The attribute with the specified QName, or <tt>null</tt> if no
	 *         such attribute could be found.
	 */
	public Att getAtt(String qName) {
		for (int i = 0; i < attributes.size(); i++) {
			Att att = attributes.get(i);

			if (att.getQName().equals(qName)) {
				return att;
			}
		}

		return null;
	}

	/**
	 * Gets the attribute with the specified namespace and local name.
	 * 
	 * @param namespace
	 *        The namespace of an attribute.
	 * @param localName
	 *        The local name of an attribute.
	 * @return The attribute with the specified namespace and local name, or
	 *         <tt>null</tt> if no such attribute could be found.
	 */
	public Att getAtt(String namespace, String localName) {
		for (int i = 0; i < attributes.size(); i++) {
			Att att = attributes.get(i);

			if (att.getLocalName().equals(localName) && att.getNamespace().equals(namespace)) {
				return att;
			}
		}

		return null;
	}

	/**
	 * Removes the attribute with the specified QName and returns it.
	 * 
	 * @param qName
	 *        The QName of an attribute.
	 * @return The removed attribute, or <tt>null</tt> if no attribute with the
	 *         specified QName could be found.
	 */
	public Att removeAtt(String qName) {
		for (int i = 0; i < attributes.size(); i++) {
			Att att = attributes.get(i);

			if (att.getQName().equals(qName)) {
				attributes.remove(i);
				return att;
			}
		}

		return null;
	}

	/**
	 * Removes the attribute with the specified namespace and local name and
	 * returns it.
	 * 
	 * @param namespace
	 *        The namespace of an attribute.
	 * @param localName
	 *        The local name of an attribute.
	 * @return The removed attribute, or <tt>null</tt> if no attribute with the
	 *         specified namespace and local name could be found.
	 */
	public Att removeAtt(String namespace, String localName) {
		for (int i = 0; i < attributes.size(); i++) {
			Att att = attributes.get(i);

			if (att.getLocalName().equals(localName) && att.getNamespace().equals(namespace)) {
				attributes.remove(i);
				return att;
			}
		}

		return null;
	}

	/**
	 * Returns the number of attributes contained in this object.
	 */
	public int size() {
		return attributes.size();
	}

	/**
	 * Produces a String-representation of this object.
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Atts[");
		for (int i = 0; i < attributes.size(); i++) {
			Att att = attributes.get(i);
			sb.append(att.getQName());
			sb.append("=");
			sb.append(att.getValue());
			sb.append("; ");
		}
		sb.append("]");
		return sb.toString();
	}
}
