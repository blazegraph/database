/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.rio.rdfxml;

/**
 * An XML attribute.
 * @openrdf
 */
class Att {

	/*-----------*
	 * Variables *
	 *-----------*/

	private String namespace;

	private String localName;

	private String qName;

	private String value;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public Att(String namespace, String localName, String qName, String value) {
		this.namespace = namespace;
		this.localName = localName;
		this.qName = qName;
		this.value = value;
	}

	/*---------*
	 * Methods *
	 *---------*/

	public String getNamespace() {
		return namespace;
	}

	public String getLocalName() {
		return localName;
	}

	public String getURI() {
		return namespace + localName;
	}

	public String getQName() {
		return qName;
	}

	public String getValue() {
		return value;
	}
}
