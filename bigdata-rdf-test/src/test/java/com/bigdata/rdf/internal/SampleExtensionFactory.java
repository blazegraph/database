package com.bigdata.rdf.internal;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.rdf.model.BigdataValue;

/**
 * Simple {@link IExtensionFactory} implementation that creates two
 * {@link IExtension}s - the {@link EpochExtension} and the 
 * {@link ColorsEnumExtension}.
 */
public class SampleExtensionFactory implements IExtensionFactory {

    private final List<IExtension<? extends BigdataValue>> extensions;
        
    public SampleExtensionFactory() {
        
        extensions = new LinkedList<IExtension<? extends BigdataValue>>(); 
            
    }
    
    @Override
    public void init(final IDatatypeURIResolver resolver,
            final ILexiconConfiguration<BigdataValue> config) {

//       	if (lex.isInlineDateTimes())
//    		extensions.add(new DateTimeExtension(
//    				lex, lex.getInlineDateTimesTimeZone()));
		extensions.add(new EpochExtension(resolver));
		extensions.add(new ColorsEnumExtension(resolver));
//		extensionsArray = extensions.toArray(new IExtension[2]);
        
    }
    
    @Override
    public Iterator<IExtension<? extends BigdataValue>> getExtensions() {
        return Collections.unmodifiableList(extensions).iterator();
    }
    
}
