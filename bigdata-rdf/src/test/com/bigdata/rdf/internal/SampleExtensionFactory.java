package com.bigdata.rdf.internal;

import java.util.Collection;
import java.util.LinkedList;

import com.bigdata.rdf.lexicon.LexiconRelation;

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
    
    public void init(final LexiconRelation lex) {

       	if (lex.isInlineDateTimes())
    		extensions.add(new DateTimeExtension(
    				lex, lex.getInlineDateTimesTimeZone()));
		extensions.add(new EpochExtension(lex));
		extensions.add(new ColorsEnumExtension(lex));
		extensionsArray = extensions.toArray(new IExtension[2]);
        
    }
    
    public IExtension[] getExtensions() {
        
        return extensionsArray;
        
    }
    
}
