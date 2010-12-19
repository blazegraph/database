package com.bigdata.rdf.internal;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Empty {@link IExtensionFactory}.
 */
public class DefaultExtensionFactory implements IExtensionFactory {

    private final Collection<IExtension> extensions;
    
    private volatile IExtension[] extensionsArray;
    
    public DefaultExtensionFactory() {
        
        extensions = new LinkedList<IExtension>(); 
            
    }
    
    public void init(final IDatatypeURIResolver resolver, 
    		final boolean inlineDateTimes) {

    	if (inlineDateTimes)
    		extensions.add(new DateTimeExtension(resolver));
		extensionsArray = extensions.toArray(new IExtension[extensions.size()]);
        
    }
    
    public IExtension[] getExtensions() {
        
        return extensionsArray;
        
    }
    
}
