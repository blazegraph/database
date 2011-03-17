package com.bigdata.rdf.internal;

import java.util.Collection;
import java.util.LinkedList;

import com.bigdata.rdf.lexicon.LexiconRelation;

/**
 * Default {@link IExtensionFactory}. The following extensions are supported:
 * <dl>
 * <dt>{@link DateTimeExtension}</dt>
 * <dd>Inlining literals which represent <code>xsd:dateTime</code> values into
 * the statement indices.</dd>
 * <dt></dt><dd></dd>
 * </dl>
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
