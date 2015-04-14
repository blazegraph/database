/* 
 * Licensed to the SYSTAP, LLC under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * SYSTAP, LLC licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed  under the  License is distributed on an "AS IS" BASIS,
 * WITHOUT  WARRANTIES OR CONDITIONS  OF ANY KIND, either  express  or
 * implied.
 * 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Created on Sep 2, 2005
 */
package junit.util;

import java.io.PrintStream;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * This utility class provides helper methods designed to fuse two
 * configurations in which at least one of the configuration is
 * represented as a {@link Properties} object, which may have a system
 * of inherited defaults.  When a {@link Map} and a {@link Properties}
 * object or two {@link Properties} objects must be combined in a
 * "fused" configuration, it may be necessary to "flatten" a {@link
 * Properties} object such that all inherited properties are placed
 * within a simple {@link Map}.  This reduces the problem to fusing
 * two maps, which may be done using {@link Map#putAll( Map other
 * )}.<p>
 *
 * Under some circumstances, it may be desirable to report a conflict
 * which would otherwise be silently ignored by {@link Map#putAll( Map
 * other )}.  A helper method has been provided to report such
 * conflicts rather than letting one {@link Map} override another.<p>
 */

public class PropertyUtil
{

    /**
     * Return a flatten copy of the specified {@link Properties}.  The
     * returned object does not share any structure with the source
     * object, but it does share key and value references.  Since keys
     * and values are {@link String}s for a {@link Properties} instance
     * this SHOULD NOT be a problem. The behavior is equivilent to:
     * <pre>
     * Properties tmp = new Properties();
     * tmp.putAll( flatten( props ) );
     * </pre>
     * 
     * @param props The properties to be flattened and copied.
     * 
     * @return A flat copy.
     */

    static public Properties flatCopy( final Properties props )
    {
        
        final Properties tmp = new Properties();

        tmp.putAll( flatten( props ) );

        return tmp;
        
    }
    
    /**
     * Return a Map that provides a flattened view of a Properties
     * object.<p>
     *
     * For each level of the Properties object, visit all keys and
     * then resolve each key against the top-level Properties object
     * placing the result into the output Map.  The order of
     * visitation of the Properties levels does not matter since the
     * value of the key is always defined against the top-level
     * Properties object which handles any defaults correctly.<p>
     *
     <pre>

     a) Example overwrites any shared keys in p1 with the definitions for
     those keys in p2.

	Properties p1 = ...;
	Properties p2 = ...;
	
	p1.putAll( flatten( p2 ) );

     b) Examples in which a Map and a Properties object are fused.

	Map m1 = ...;
	Properties p1 = ....;

	// Override m1 with p1.
	m1.putAll( flatten( p1 ) );

	vs.

	// Override p1 with m1.
	p1.putAll( m1 );

	</pre>
    */	
	
    public static Map flatten( Properties properties )
    {

	if( properties == null ) {

	    throw new IllegalArgumentException();

	}

	Map out = new TreeMap();

	Enumeration e = properties.propertyNames();

	while( e.hasMoreElements() ) {

	    String property = (String) e.nextElement();

	    String propertyValue = properties.getProperty
		( property
		  );

	    out.put( property,
		     propertyValue
		     );

	}

	return out;

    }

    /**
     * Lists all entries defined either directly by a {@link Properties}
     * object or at any level within its defaults hierarchy.
     */

    static public void list( String msg, Properties properties, PrintStream ps )
    {

        ps.println( msg+"-- listing properties --" );
        
        Enumeration e = properties.propertyNames();

	while( e.hasMoreElements() ) {

	    String property = (String) e.nextElement();

	    String propertyValue = properties.getProperty
		( property
		  );

	    ps.println( property + "=" + propertyValue );

	}
    
    }
    
    /**
     * Fuses two configurations and ignores any conflicts.
     * 
     * @param defaults The default configuration.
     *
     * @param override Another configuration whose values will be
     * fused with the <i>defaults</i>.  If this is a {@link
     * Properties} object then it is first flattened so that any
     * inherited property values will be fused.
     *
     * @return A {@link Properties} object.  If <i>defaults</i> was a
     * {@link Properties} object, then this is <i>defaults</i> and any
     * values from <i>override</i> have been added to <i>defaults</i>.
     * Otherwise a new {@link Properties} object is created, the
     * entries from <i>defaults</i> are copied into that {@link
     * Properties} object, and any values from <i>override</i> are
     * copied onto that {@link Properties} object.
     */

    static public Properties fuse( Map defaults, Map override )
    {

	final boolean ignoreConflicts = true;

	return fuse( defaults, override, ignoreConflicts );

    }

    /**
     * Fuses two configurations and optionally reports any conflicts.
     * 
     * @param defaults The default configuration.
     *
     * @param override Another configuration whose values will be
     * fused with the <i>defaults</i>.  If this is a {@link
     * Properties} object then it is first flattened so that any
     * inherited property values will be fused.
     *
     * @param ignoreConflicts When true an exception is not
     * reported if an entry from <i>override</i> would override an
     * entry in <i>defaults</i>.
     *
     * @return A {@link Properties} object.  If <i>defaults</i> was a
     * {@link Properties} object, then this is <i>defaults</i> and any
     * values from <i>override</i> have been added to <i>defaults</i>.
     * Otherwise a new {@link Properties} object is created, the
     * entries from <i>defaults</i> are copied into that {@link
     * Properties} object, and any values from <i>override</i> are
     * copied onto that {@link Properties} object.
     */

    static public Properties fuse( Map defaults, Map override, boolean ignoreConflicts )
    {

	if( defaults == null ) {

	    throw new IllegalArgumentException();

	}

	if( override == null ) {

	    throw new IllegalArgumentException();

	}

	if( override instanceof Properties ) {

	    override = flatten
		( (Properties) override
		  );

	}

	if( ! ( defaults instanceof Properties ) ) {

	    Properties tmp = new Properties();
	    
	    tmp.putAll( defaults );
	    
	    defaults = tmp;

	}
    
	list( "defaults : ", ((Properties)defaults), System.err );

	// Visit all entries in the [override] Map (it is a Map since
	// we flattened it if it was a Properties object).
	Iterator itr = override.entrySet().iterator();
	
	while( itr.hasNext() ) {

	    Map.Entry entry = (Map.Entry) itr.next();

	    String property = (String) entry.getKey();
	    
	    String overrideValue = (String) entry.getValue();

	    String existingValue = ((Properties)defaults).getProperty
		( property
		  );

	    System.err.println
	    	( "property="+property+" : existingValue="+existingValue+", overrideValue="+overrideValue );
	    
	    if( existingValue != null ) {

		if( existingValue.equals( overrideValue ) ) {

		    // Property already has this value.

		    continue;

		} else if( ignoreConflicts ) {

		    // Override the existing value.

		    defaults.put( property, overrideValue );

		} else {

		    // Throw exception rather than overriding the
		    // existing value for that property.

		    throw new RuntimeException
			( "Would override property="+property+
			  ": existingValue="+existingValue+
			  ", overrideValue="+overrideValue
			  );

		}

	    } else {

		// Since the property was not defined, nothing could
		// be overriden and we just copy the value from the
		// [override] source.

		defaults.put
		    ( property,
		      overrideValue
		      );

	    }

	}

	// Return a Properties object.  If [defaults] was a Properties
	// object, then this is [defaults].  Otherwise [defaults] has
	// been wrapped up as a {@link Properties} object and any
	// values from [overriden] were copied onto [defaults].

	return (Properties) defaults;

    }

//     /**
//      * Helper class wraps an existing {@link Properties} object and
//      * exposes its {@link Properties#defaults} field.
//      */

//     private static class MyProperties
//     	extends Properties
//     {

// 	public MyProperties( Properties properties )
// 	{

// 	    super( properties );

// 	}

// 	public Properties getDefaults()
// 	{

// 	    return defaults;

// 	}

//     }

    
    public static Properties convert( Map configParams )
    {
        
        Properties properties = null;

        if( configParams instanceof Properties ) {

            // Make sure that we inherit default properties if we were
            // passed a Properties object.

            properties = new Properties
                ( (Properties) configParams
                  );

        } else {

            // If we were NOT passed a Properties object, then we
            // allocate one and initialize it from the Map.

            properties = new Properties();

            properties.putAll
                ( configParams
                  );

        }
        
        return properties;
        
    }
    
    
}
