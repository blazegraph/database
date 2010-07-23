package com.bigdata.rdf.internal;

public class DefaultExtensionFactory implements IExtensionFactory {

    private final IExtension[] extensions;
    
    public DefaultExtensionFactory(final IDatatypeURIResolver resolver) {
        
        extensions = new IExtension[] {
            new EpochExtension(resolver),
            new ColorsEnumExtension(resolver)
        };
        
    }
    
    public IExtension[] getExtensions() {
        
        return extensions;
        
    }
    
}
