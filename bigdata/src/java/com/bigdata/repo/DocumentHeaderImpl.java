package com.bigdata.repo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.repo.BigdataRepository.MetadataSchema;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

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
     
        properties = new HashMap<String,Object>();
        
        Iterator<PropertyValue> itr = header.propertyValues();
        
        while(itr.hasNext()) {
            
            PropertyValue tmp = itr.next();
            
            properties.put(tmp.getName(), tmp.getValue());
            
        }
        
    }
    
    public void setId(String id) {

        properties.put(MetadataSchema.ID, id);

    }

    /**
     * Package private method sets the file version.
     * 
     * @param version
     *            The file version.
     */
    void getVersion(int version) {
        
        properties.put(MetadataSchema.VERSION,Integer.valueOf(version));
        
    }

    public void setContentType(String contentType) {

        properties.put(MetadataSchema.CONTENT_TYPE, contentType);

    }
    
    public void setContentEncoding(String contentEncoding) {

        properties.put(MetadataSchema.CONTENT_ENCODING, contentEncoding);

    }

    public String getId() {

        return (String) properties.get(MetadataSchema.ID);
        
    }

    public int getVersion() {
        
        Integer version = (Integer) properties.get(MetadataSchema.VERSION);

        if (version == null)
            throw new IllegalStateException("No version");
        
        return version.intValue();
        
    }
    
    public String getContentType() {

        return (String) properties.get(MetadataSchema.CONTENT_TYPE);

    }

    public String getContentEncoding()
    {
        
        return (String) properties.get(MetadataSchema.CONTENT_ENCODING);
        
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
    
    public Iterator<PropertyValue> propertyValues() {

        return new Striterator(properties.entrySet().iterator())
                .addFilter(new Resolver() {

                    private static final long serialVersionUID = 1L;

                    protected Object resolve(Object arg0) {

                        Map.Entry<String, Object> entry = (Map.Entry<String, Object>) arg0;

                        return new PropertyValueImpl(entry.getKey(), entry
                                .getValue());

                    }
                });

    }

}
 