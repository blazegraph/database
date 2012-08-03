package com.bigdata.gom.alchemy.owl;

import java.util.Iterator;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.gom.gpo.BasicSkin;
import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.IGenericSkin;
import com.bigdata.gom.om.IObjectManager;

public class OWLClassSkin extends BasicSkin implements IGenericSkin {

	public OWLClassSkin(IGPO gpo) {
		super(gpo);
	}

	/**
	 * Returns a list of defined OWLClasses.  The classes do not
	 * in fact have any reference to the Ontology instance, but the
	 * skin supports the fiction.
	 */
	public Iterator<OWLPropertySkin> getProperties() {
		final Iterator<IGPO> owlProperties = m_gpo.getLinksIn(RDFS.DOMAIN).iterator();
		
		return new Iterator<OWLPropertySkin>() {

			@Override
			public boolean hasNext() {
				return owlProperties.hasNext();
			}

			@Override
			public OWLPropertySkin next() {
				IGPO nxt = owlProperties.next();
				return (OWLPropertySkin) ((GPO) nxt).getSkin(OWLPropertySkin.class);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};		
	}

	public String getName() {
		return m_gpo.getId().stringValue();
	}

	public Iterator<OWLClassSkin> getSubclasses() {
		final Iterator<IGPO> subclasses = m_gpo.getLinksIn(RDFS.SUBCLASSOF).iterator();
		
		return new Iterator<OWLClassSkin>() {

			@Override
			public boolean hasNext() {
				return subclasses.hasNext();
			}

			@Override
			public OWLClassSkin next() {
				IGPO nxt = subclasses.next();
				return (OWLClassSkin) ((GPO) nxt).getSkin(OWLClassSkin.class);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};		
	}
}
