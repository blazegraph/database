package com.bigdata.rdf.internal;

import java.util.Collection;
import java.util.LinkedList;

import com.bigdata.rdf.lexicon.LexiconRelation;

/**
 * Empty {@link IExtensionFactory}.
 */
public class DefaultExtensionFactory implements IExtensionFactory {

    private final Collection<IExtension> extensions;
    
    private volatile IExtension[] extensionsArray;
    
    public DefaultExtensionFactory() {
        
        extensions = new LinkedList<IExtension>(); 
            
    }
    
    public void init(final LexiconRelation lex) {

    	if (lex.isInlineDateTimes())
    		extensions.add(new DateTimeExtension(
    				lex, lex.getInlineDateTimesTimeZone()));
    	
		extensionsArray = extensions.toArray(new IExtension[extensions.size()]);
        
    }
    
    public IExtension[] getExtensions() {
        
        return extensionsArray;
        
    }
    
}
