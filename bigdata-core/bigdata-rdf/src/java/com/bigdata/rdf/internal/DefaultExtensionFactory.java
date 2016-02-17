package com.bigdata.rdf.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.internal.impl.extensions.DerivedNumericsExtension;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension;
import com.bigdata.rdf.internal.impl.extensions.XSDStringExtension;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.service.GeoSpatialConfig;

/**
 * Default {@link IExtensionFactory}. The following extensions are supported:
 * <dl>
 * <dt>{@link DateTimeExtension}</dt>
 * <dd>Inlining literals which represent <code>xsd:dateTime</code> values into
 * the statement indices.</dd>
 * <dt>{@link XSDStringExtension}</dt>
 * <dd>Inlining <code>xsd:string</code> literals into the statement indices.</dd>
 * <dt>{@link DerivedNumericsExtension}</dt>
 * <dd>Inlining literals which represent derived numeric values into
 * the statement indices.</dd>
 * </dl>
 */
public class DefaultExtensionFactory implements IExtensionFactory {

    private final List<IExtension<? extends BigdataValue>> extensions;
    
    public DefaultExtensionFactory() {
        
        extensions = new LinkedList<IExtension<? extends BigdataValue>>(); 
            
    }
    
    @Override
    public void init(final IDatatypeURIResolver resolver,
            final ILexiconConfiguration<BigdataValue> config) {

    	/*
    	 * Always going to inline the derived numeric types.
    	 */
    	extensions.add(new DerivedNumericsExtension<BigdataLiteral>(resolver));
    	
    	if (config.isGeoSpatial()) {
    	   
    	   // initialize the GeoSpatialConfig object
    	   GeoSpatialConfig.getInstance().init(config.getGeoSpatialConfig());
         extensions.add(new GeoSpatialLiteralExtension<BigdataLiteral>(resolver));    	
      }
    	
    	if (config.isInlineDateTimes()) {
    		
    		extensions.add(new DateTimeExtension<BigdataLiteral>(
    				resolver, config.getInlineDateTimesTimeZone()));
    		
    	}

        if (config.getMaxInlineStringLength() > 0) {
			/*
			 * Note: This extension is used for both literals and URIs. It MUST
			 * be enabled when MAX_INLINE_TEXT_LENGTH is GT ZERO (0). Otherwise
			 * we will not be able to inline either the local names or the full
			 * text of URIs.
			 */
            extensions.add(new XSDStringExtension<BigdataLiteral>(resolver, config
                    .getMaxInlineStringLength()));
        }
        
        _init(resolver, config, extensions);
        
    }
    
    /**
     * Give subclasses a chance to add extensions.
     * 
     * @param resolver
     *            {@link IDatatypeURIResolver} from
     *            {@link #init(IDatatypeURIResolver, ILexiconConfiguration)}.
     * @param config
     *            The {@link ILexiconConfiguration} from
     *            {@link #init(IDatatypeURIResolver, ILexiconConfiguration)}.
     * 
     * @param extensions
     *            The extensions that have already been registered.
     * 
     * @see #init(IDatatypeURIResolver, ILexiconConfiguration)
     */
    protected void _init(final IDatatypeURIResolver resolver,
            final ILexiconConfiguration<BigdataValue> config,
            final Collection<IExtension<? extends BigdataValue>> extensions) {

    	// noop
    	
    }
    
    @Override
    public Iterator<IExtension<? extends BigdataValue>> getExtensions() {
        return Collections.unmodifiableList(extensions).iterator();
    }
    
}
