package com.bigdata.rdf.sail.config;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Defines constants for the BigdataSail schema which is used by
 * {@link BigdataStoreFactory}s to initialize {@link BigdataSail}s.
 */
public class BigdataStoreSchema {

	/** The BigdataSail schema namespace 
     * (<tt>http://www.bigdata.com/config/sail/bigdata#</tt>). 
     */
	public static final String NAMESPACE = 
            "http://www.bigdata.com/config/sail/bigdata#";

	/** <tt>http://www.bigdata.com/config/sail/bigdata#properties</tt> */
	public final static URI PROPERTIES;

	static {
		ValueFactory factory = ValueFactoryImpl.getInstance();
		PROPERTIES = factory.createURI(NAMESPACE, "properties");
	}
}
