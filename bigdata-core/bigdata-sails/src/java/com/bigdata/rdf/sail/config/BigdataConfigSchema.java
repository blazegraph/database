package com.bigdata.rdf.sail.config;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Defines constants for the schema which is used by
 * {@link BigdataSailFactory} and {@link BigdataRepositoryFactory}.
 */
public class BigdataConfigSchema {

	/** The bigdata schema namespace 
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
