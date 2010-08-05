package com.bigdata.rdf.internal;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Simple {@link IExtensionFactory} implementation that creates two
 * {@link IExtension}s - the {@link EpochExtension} and the 
 * {@link ColorsEnumExtension}.
 */
public class SampleExtensionFactory implements IExtensionFactory {

    private final Collection<IExtension> extensions;
    
    private volatile IExtension[] extensionsArray;
    
    public SampleExtensionFactory() {
        
        extensions = new LinkedList<IExtension>(); 
            
    }
    
    public void init(final IDatatypeURIResolver resolver) {

		extensions.add(new EpochExtension(resolver));
		extensions.add(new ColorsEnumExtension(resolver));
		extensionsArray = extensions.toArray(new IExtension[2]);
        
    }
    
    public IExtension[] getExtensions() {
        
        return extensionsArray;
        
    }
    
}
