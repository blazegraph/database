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

public class OWLOntologySkin extends BasicSkin implements IGenericSkin {

	public OWLOntologySkin(IGPO gpo) {
		super(gpo);
	}

	
	static public OWLOntologySkin getOntology(IObjectManager om) {
		final IGPO owl = om.getGPO(OWL.ONTOLOGY);

		return (OWLOntologySkin) ((GPO) owl).getSkin(OWLOntologySkin.class);
	}

	/**
	 * Returns a list of defined OWLClasses.  The classes do not
	 * in fact have any reference to the Ontology instance, but the
	 * skin supports the fiction.
	 */
	public Iterator<OWLClassSkin> getClasses() {
		final IObjectManager om = m_gpo.getObjectManager();
		
		final IGPO classClass = om.getGPO(OWL.CLASS);
		
		final Iterator<IGPO> owlClasses = classClass.getLinksIn(RDF.TYPE).iterator();
		
		return new Iterator<OWLClassSkin>() {

			@Override
			public boolean hasNext() {
				return owlClasses.hasNext();
			}

			@Override
			public OWLClassSkin next() {
				IGPO nxt = owlClasses.next();
				return (OWLClassSkin) ((GPO) nxt).getSkin(OWLClassSkin.class);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};		
	}
}
