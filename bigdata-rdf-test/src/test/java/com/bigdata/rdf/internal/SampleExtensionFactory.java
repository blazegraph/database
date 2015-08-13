package com.bigdata.rdf.internal;

import java.util.Collection;
import java.util.LinkedList;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;

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
    
    @Override
    public void init(final IDatatypeURIResolver resolver,
            final ILexiconConfiguration<BigdataValue> config) {

//       	if (lex.isInlineDateTimes())
//    		extensions.add(new DateTimeExtension(
//    				lex, lex.getInlineDateTimesTimeZone()));
		extensions.add(new EpochExtension(resolver));
		extensions.add(new ColorsEnumExtension(resolver));
		extensionsArray = extensions.toArray(new IExtension[2]);
        
    }
    
    @Override
    public IExtension[] getExtensions() {
        
        return extensionsArray;
        
    }
    
}
