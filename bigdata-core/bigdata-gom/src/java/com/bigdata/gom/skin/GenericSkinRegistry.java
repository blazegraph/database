/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package com.bigdata.gom.skin;

// behavior skin registery.
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.IGenericSkin;
import com.bigdata.gom.gpo.ILinkSet;

/**
 * <p>Helper class provides a global (JVM wide) registery for {@link
 * IGenericSkin}s.  Generally, when writing an {@link IGenericSkin} or
 * {@link ILinkSetSkin} that implements some interface, you will
 * include a static initialization block in the implementation class
 * that registers the skin using {@link #registerClass( Class theClass
 * )}.</p>
 * <p>
 * Note: Registered {@link IGenericSkin} implementations that are to
 * be instantiated for a backing {@link IGPO} MUST provide a
 * public constructor accepting a single {@link IGenericSkin}
 * argument.</p>
 * <p>
 * Note: Registered {@link ILinkSetSkin} implementations that are to
 * be instantiated for a backing {@link ILinkSet} MUST provide a
 * public constructor accepting a single {@link ILinkSetSkin}
 * argument.</p>
 */

public class GenericSkinRegistry
{

    /**
     * The {@link Logger} for the {@link GenericSkinRegistry}.
     */

    public static final Logger log = Logger.getLogger
    	( GenericSkinRegistry.class
	  );

    static Collection m_registeredClass = 
        Collections.synchronizedCollection( new Vector() );

    /**
     * This static method MUST be used to register any {@linkIGenericSkin}s.
     * Once a skin has been registered, you can use
     * {@link IGPO#asClass( Class theClassOrInterface )} or {@link
     * ILinkSet#linkSetAsClass( Class theClassOrInterface )} to re-skin any
     * {@link IGPO} object or {@link ILinkSet} by naming the interface (or
     * implementation class) that you want to expose for the persistent data.
     * <p>
     * 
     * @param theClass
     *            The implementation class.
     * 
     * @todo Consider that more than one implementation class for the same
     *       interface may be registered. This should at LEAST generate a
     *       warning since only the first such implementation class for an
     *       interface will be used.
     * 
     * @exception IllegalArgumentException
     *                If <i>theClass </i> does not implement either
     *                {@link IGenericSkin} or {@link ILinkSetSkin}.
     */

    public static void registerClass
	( Class theClass
	  )
    {

	if( theClass.isInterface() ) {

	    log.error
		( "This is an interface, not an implementation class: "+
		  theClass.getName()
		  );

	    throw new IllegalArgumentException
		( "This is an interface, not an implementation class: "+
		  theClass.getName()
		  );

	}

	if( ! IGenericSkin.class.isAssignableFrom( theClass ) //&&
//	    ! ILinkSetSkin.class.isAssignableFrom( theClass )
	    ) {

// 	    log.error
// 		( "Implementation classes MUST implement: "+
// 		  IGenericSkin.class.getName()
// 		  );

	    throw new IllegalArgumentException
		( "Class MUST implement: "+
		  IGenericSkin.class.getName()
//		  +" -or- "+
//		  ILinkSetSkin.class.getName()
		  );

	}

	/* Check to see if the skin has already been registered.  If
	 * not, then we register it now.
	 */

	if( m_registeredClass.contains( theClass ) ) {

//	    log.info
//		( "Skin already registered: "+theClass
//		  );

	} else {

	    m_registeredClass.add
		( theClass
		  );

	    log.info
		( "Registered skin: "+theClass
		  );

	}
	    
    }

    /**
     * Returns a {@link Class} that has been registered using {@link
     * #registerClass( Class theClassOrInterface )} as a skin.
     * 
     * @param theClassOrInterface
     * 
     * @return The implementation class.
     * 
     * @exception UnsupportedOperationException if there is no registered
     * skin identified by the specified class or interface.
     * 
     * @exception IllegalArgumentException if <i>theClassOrInterface</i> is
     * <code>null</code>.
     */
    
    public static Class getImplementationClass
        ( Class theClassOrInterface
          )
    {
        
        if( theClassOrInterface == null ) {

            throw new IllegalArgumentException
                ( "The class or interface may not be null."
                  );

        }

        Class theImplementationClass = null;

        if( theClassOrInterface.isInterface() ) {

            /* We need to find a registered implementation class for
             * this interface.
             */

            for( Iterator it = m_registeredClass.iterator(); it.hasNext(); ) {
                
                Class aClass = ( Class ) it.next();

                if( theClassOrInterface.isAssignableFrom( aClass ) ) {

                    theImplementationClass = aClass;

                    break;

                }

            }

            if( theImplementationClass == null ) {

                throw new UnsupportedOperationException
                    ( "No implementation class has been registered for that interface: "+theClassOrInterface.getName()
                      );

            }
            
        } else if( m_registeredClass.contains( theClassOrInterface ) ) {

            /*
             * Otherwise this is already an implementation class.
             * 
             * @todo Rather than requiring the Class to have been registered, we
             * could simply register the class now.
             */

            theImplementationClass = theClassOrInterface;

        } else {
            
            throw new UnsupportedOperationException
                ( "No registered class matches or is assignable from the parameter: "+theClassOrInterface.getName()
                  );
            
        }

        return theImplementationClass;
        
    }
    
    /**
     * Helper method for {@link IGPO#asClass( Class
     * theClassOrInterface )} implementations.<p>
     *
     * @return A {@link IGenericSkin} for <i>g</i> that is an
     * instance of the desired class or that implements the desired
     * interface.
     *
     * @exception UnsupportedOperationException if a suitable {@link
     * IGenericSkin} did not exist and could not be minted.  Typically
     * this means that no suitable implementation class has been registered,
     * or that the class does not implement a suitable constructor.
     */

    public static IGenericSkin asClass
	( IGPO g,
	  Class theClassOrInterface
	  )
	throws UnsupportedOperationException
    {

	if( g == null ) {

	    log.error
		( "The generic object may not be null."
		  );

	    throw new IllegalArgumentException
		( "The generic object may not be null."
		  );

	}

	if( theClassOrInterface == null ) {

	    log.error
		( "The class or interface may not be null."
		  );

	    throw new IllegalArgumentException
		( "The class or interface may not be null."
		  );

	}

	/* If _this_ object already implements the desired behavior
	 * then just return it.
	 *
	 * Note: If you are using the recommended pattern of
	 * delegating the IGeneric interface to a concrete
	 * implementation of that interface, then this will never be
	 * true.  (Think about it as if this class were final - there
	 * would be no other interfaces that it could implement).
	 */

	if( theClassOrInterface.isInstance( g ) ) {

	    return g;

	}

	Class theImplementationClass = getImplementationClass
            ( theClassOrInterface 
              );

	/* One way or another, we now have an implementation class.
	 * We test to make sure that there is an appropriate public
	 * constructor that will accept the IGeneric interface as its
	 * sole parameter.
	 */

	try {

	    return mintGenericSkin( theImplementationClass, g );
	    
	}

	catch( Throwable t ) {

	    log.error
		( "Unable to morph behavior to"+
		  ": theClassOrInterface="+theClassOrInterface,
		  t
		  );

	    RuntimeException ex = new UnsupportedOperationException
		( "Unable to morph behavior to"+
		  ": theClassOrInterface="+theClassOrInterface+
		  ", initCause="+t
		  );

	    ex.initCause( t );

	    throw ex;

	}

    }


//    /**
//     * Helper method for {@link ILinkSet#linkSetAsClass( Class
//     * theClassOrInterface )} implementations.<p>
//     * 
//     * @return A {@link ILinkSetSkin} for <i>ls</i> that is an
//     * instance of the desired class or implements the desired
//     * interface.
//     *
//     * @exception UnsupportedOperationException if a suitable {@link
//     * ILinkSetSkin} did not exist and could not be minted.  Typically
//     * this means that no suitable implementation class has been registered,
//     * or that the class does not implement a suitable constructor.
//     */
//
//    public static ILinkSetSkin asClass
//	( ILinkSet ls,
//	  Class theClassOrInterface
//	  )
//	throws UnsupportedOperationException
//    {
//
//	if( ls == null ) {
//
//	    log.error
//		( "The link set may not be null."
//		  );
//
//	    throw new IllegalArgumentException
//		( "The link set not be null."
//		  );
//
//	}
//
//	if( theClassOrInterface == null ) {
//
//	    log.error
//		( "The class or interface may not be null."
//		  );
//
//	    throw new IllegalArgumentException
//		( "The class or interface may not be null."
//		  );
//
//	}
//
//	/* If _this_ object already implements the desired behavior
//	 * then just return it.
//	 *
//	 * Note: If you are using the recommended pattern of
//	 * delegating the ILinkSet interface to a concrete
//	 * implementation of that interface, then this will never be
//	 * true.  (Think about it as if this class were final - there
//	 * would be no other interfaces that it could implement).
//	 */
//
//	if( theClassOrInterface.isInstance( ls ) ) {
//
//	    return ls;
//
//	}
//
//        Class theImplementationClass = getImplementationClass
//            ( theClassOrInterface 
//              );
//
//	/* One way or another, we now have an implementation class.
//	 * We test to make sure that there is an appropriate public
//	 * constructor that will accept the ILinkSet interface as its
//	 * sole parameter.
//	 */
//
//	try {
//
//	    return mintLinkSetSkin( theImplementationClass, ls );
//	    
//	}
//
//	catch( Throwable t ) {
//
//	    log.error
//		( "Unable to morph behavior to"+
//		  ": "+theClassOrInterface
//		  );
//
//	    RuntimeException ex = new UnsupportedOperationException
//		( "Unable to morph behavior to"+
//		  ": "+theClassOrInterface
//		  );
//
//	    ex.initCause( t );
//
//	    throw ex;
//
//	}
//
//    }
    
    public static IGenericSkin mintGenericSkin( Class implClass, IGPO g ) 
        throws Exception
    {
        
        Constructor c = implClass.getDeclaredConstructor
            ( new Class[] { IGPO.class }
              );
        
        return (IGenericSkin) c.newInstance
            ( new Object[] { g }
              );
        
    }

//    public static ILinkSetSkin mintLinkSetSkin( Class implClass, ILinkSet ls ) 
//        throws Exception
//    {
//        
//        Constructor c = implClass.getDeclaredConstructor
//            ( new Class[] { ILinkSetSkin.class }
//              );
//        
//        return (ILinkSetSkin) c.newInstance
//            ( new Object[] { ls }
//              );
//        
//    }

}
