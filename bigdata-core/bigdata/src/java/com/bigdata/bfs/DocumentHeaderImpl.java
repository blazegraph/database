package com.bigdata.bfs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Flyweight {@link DocumentHeader} implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DocumentHeaderImpl implements DocumentHeader 
{

    protected static Logger log = Logger.getLogger(DocumentHeaderImpl.class);

    final private Map<String,Object> properties;
    
    /**
     * Create a new empty document.
     */
    public DocumentHeaderImpl()
    {
    
        properties = new HashMap<String,Object>();
        
    }
    
    /**
     * Copy constructor for header information.
     * 
     * @param header to copy
     */
    public DocumentHeaderImpl( DocumentHeader header )
    {
     
        this(header.asMap());
        
    }
    
    public DocumentHeaderImpl( Map<String,Object> metadata )
    {
     
        properties = new HashMap<String,Object>(metadata);
        
    }
    
    public void setId(String id) {

        properties.put(FileMetadataSchema.ID, id);

    }

    /**
     * Package private method sets the file version.
     * 
     * @param version
     *            The file version.
     */
    void getVersion(int version) {
        
        properties.put(FileMetadataSchema.VERSION,Integer.valueOf(version));
        
    }

    public void setContentType(String contentType) {

        properties.put(FileMetadataSchema.CONTENT_TYPE, contentType);

    }
    
    public void setContentEncoding(String contentEncoding) {

        properties.put(FileMetadataSchema.CONTENT_ENCODING, contentEncoding);

    }

    public String getId() {

        return (String) properties.get(FileMetadataSchema.ID);
        
    }

    public int getVersion() {
        
        Integer version = (Integer) properties.get(FileMetadataSchema.VERSION);

        if (version == null)
            throw new IllegalStateException("No version");
        
        return version.intValue();
        
    }
    
    public String getContentType() {

        return (String) properties.get(FileMetadataSchema.CONTENT_TYPE);

    }

    public String getContentEncoding()
    {
        
        return (String) properties.get(FileMetadataSchema.CONTENT_ENCODING);
        
    }

    /**
     * Set an arbitrary property value.
     * 
     * @param property
     *            The property name.
     * @param value
     *            The value.
     */
    public void setProperty(String name,Object newValue) {
        
        properties.put(name,newValue);
        
    }

    public Object getProperty(String name) {
        
        return properties.get(name); 
        
    }

    public Map<String,Object> asMap() {
        
        return Collections.unmodifiableMap(properties);
        
    }

    /**
     * Always returns <code>false</code>.
     */
    public boolean exists() {
 
        return false;
        
    }

    /**
     * @throws IllegalStateException always.
     */
    public long getEarliestVersionCreateTime() {
        
        throw new IllegalStateException();
        
    }

    /**
     * @throws IllegalStateException always.
     */
    public long getMetadataUpdateTime() {
        
        throw new IllegalStateException();
        
    }

    /**
     * @throws IllegalStateException always.
     */
    public long getVersionCreateTime() {
        
        throw new IllegalStateException();
        
    }

}
