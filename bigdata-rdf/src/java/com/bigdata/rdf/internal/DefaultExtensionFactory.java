package com.bigdata.rdf.internal;

/**
 * Empty {@link IExtensionFactory}.
 */
public class DefaultExtensionFactory implements IExtensionFactory {

    private final IExtension[] extensions;
    
    public DefaultExtensionFactory() {
        
        extensions = new IExtension[0];
        
    }
    
    public void init(final IDatatypeURIResolver resolver) {
        
    }
    
    public IExtension[] getExtensions() {
        
        return extensions;
        
    }
    
}
