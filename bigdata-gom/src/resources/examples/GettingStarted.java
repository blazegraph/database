/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 30, 2005
 */

import java.util.Properties;

import org.CognitiveWeb.generic.IGeneric;
import org.CognitiveWeb.generic.IObjectManager;
import org.CognitiveWeb.generic.ObjectManagerFactory;
import org.CognitiveWeb.generic.core.om.ObjectManager;
import org.CognitiveWeb.generic.core.om.RuntimeOptions;

/**
 * Simple example shows how to create an object manager, some generic objects,
 * and set some generic and link properties.
 * 
 * @author thompsonbry
 * @version $Id$
 * 
 * @release Update package.xml example for new approach to creating an object
 *          manager with a specified persistence layer. Note that there is no
 *          longer a persistence layer integration bundled with the
 *          generic-native module. People should be refered to one of the GOM
 *          integration modules instead.
 */

public class GettingStarted {

    /**
     * 
     */
    public GettingStarted()
    {
        
	  Properties properties = new Properties();
	  
	  properties.setProperty(ObjectManagerFactory.OBJECT_MANAGER_CLASSNAME,
				ObjectManager.class.getName());

	  properties.setProperty(RuntimeOptions.PERSISTENT_STORE,
				RuntimeOptions.PERSISTENT_STORE_JDBM);

	  IObjectManager om  = ObjectManagerFactory.INSTANCE.newInstance
	      ( properties
	        );

	  IGeneric g = om.makeObject();

	  g.set( "name", "foo" );

	  g.set( "father", om.makeObject() );

	  om.close();
	  
    }
    
    static public void main(String[] args) {
    	
        new GettingStarted();
    }

}
