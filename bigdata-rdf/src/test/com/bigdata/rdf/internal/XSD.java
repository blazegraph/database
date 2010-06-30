package com.bigdata.rdf.internal;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import com.bigdata.rdf.model.BigdataURIImpl;

public interface XSD {
    
    String NAMESPACE = "http://www.w3.org/2001/XMLSchema#";
    
    URI BOOLEAN = new URIImpl(NAMESPACE
            + "boolean");

    URI BYTE = new URIImpl(NAMESPACE
            + "byte");

    URI SHORT = new URIImpl(NAMESPACE
            + "short");

    URI INT = new URIImpl(NAMESPACE
            + "int");

    URI LONG = new URIImpl(NAMESPACE
            + "long");

    URI UNSIGNED_BYTE = new URIImpl(NAMESPACE
            + "unsignedByte");

    URI UNSIGNED_SHORT = new URIImpl(NAMESPACE
            + "unsignedShort");

    URI UNSIGNED_INT = new URIImpl(NAMESPACE
            + "unsignedInt");

    URI UNSIGNED_LONG = new URIImpl(NAMESPACE
            + "unsignedLong");
    
    URI FLOAT = new URIImpl(NAMESPACE
            + "float");

    URI DOUBLE = new URIImpl(NAMESPACE
            + "double");

    URI INTEGER = new URIImpl(NAMESPACE
            + "integer");

    URI DECIMAL = new URIImpl(NAMESPACE
            + "decimal");

    URI UUID = new URIImpl(NAMESPACE
            + "uuid");

    
}
