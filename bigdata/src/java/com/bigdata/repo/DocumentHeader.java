package com.bigdata.repo;

import java.util.Iterator;

/**
 * Document header interface for {@link Document}s stored in the
 * {@link ContentRepository}.
 * 
 * @author mike@systap.com
 * @version $Id$
 */
public interface DocumentHeader
{

    /**
     * The unique identifier for the document within the
     * {@link ContentRepository}.
     */
    String getId();
    
//    /**
//     * The #of bytes in the encoded content.
//     */
//    long getContentLength();
    
    /**
     * The MIME type for the document.
     */
    String getContentType();

    /**
     * The character set encoding that MUST be used to interpret the byte[]
     * returned by {@link Document#getContent()}.
     */
    String getContentEncoding();
    

    /**
     * Get an arbitrary property value.
     * 
     * @param property
     *            The property name.
     *            
     * @return The value.
     */
    Object getProperty(String name);
    
    /**
     * Visits all defined property values.
     */
    Iterator<PropertyValue> propertyValues();
    
}
