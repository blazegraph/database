package com.bigdata.rdf.internal;

/**
 * Simple {@link IExtensionFactory} implementation that creates two
 * {@link IExtension}s - the {@link EpochExtension} and the 
 * {@link ColorsEnumExtension}.
 */
public class SampleExtensionFactory implements IExtensionFactory {

    private final IExtension[] extensions;
    
    public SampleExtensionFactory() {
        
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
