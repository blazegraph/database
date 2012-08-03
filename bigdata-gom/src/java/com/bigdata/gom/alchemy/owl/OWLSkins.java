package com.bigdata.gom.alchemy.owl;

import org.openrdf.model.vocabulary.OWL;

import com.bigdata.gom.gpo.GPO;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.skin.GenericSkinRegistry;

/**
 * This is just a hook class to register the OWL GPO skins.
 * @author Martyn Cutcher
 *
 */
public class OWLSkins {

	static public void register() {
		GenericSkinRegistry.registerClass(OWLOntologySkin.class);
		GenericSkinRegistry.registerClass(OWLClassSkin.class);
		GenericSkinRegistry.registerClass(OWLPropertySkin.class);
	}
}
