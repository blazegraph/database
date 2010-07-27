package com.bigdata.rdf.internal;

/**
 * Empty {@link IExtensionFactory}.
 */
public class DefaultExtensionFactory implements IExtensionFactory {

    private final IExtension[] extensions;
    
    public DefaultExtensionFactory() {
        
        extensions = new IExtension[] {
        };
        
    }
    
    public void resolveDatatypes(final IDatatypeURIResolver resolver) {
        
        for (IExtension extension : extensions) {
            extension.resolveDatatype(resolver);
        }
        
    }
    
    public IExtension[] getExtensions() {
        
        return extensions;
        
    }
    
}
