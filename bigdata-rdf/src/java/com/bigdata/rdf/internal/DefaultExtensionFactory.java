package com.bigdata.rdf.internal;

/**
 * Simple {@link IExtensionFactory} implementation that creates two
 * {@link IExtension}s - the {@link EpochExtension} and the 
 * {@link ColorsEnumExtension}.
 */
public class DefaultExtensionFactory implements IExtensionFactory {

    private final IExtension[] extensions;
    
    public DefaultExtensionFactory() {
        
        extensions = new IExtension[] {
            new EpochExtension(),
            new ColorsEnumExtension()
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
