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
package com.bigdata.gom;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import junit.framework.TestCase2;

/**
 * Note: Java6 embeds JavaScript support based on Mozilla Rhino version 1.6R2.
 * 
 * @see <a
 *      href="http://docs.oracle.com/javase/6/docs/technotes/guides/scripting/programmer_guide/index.html">
 *      Scripting Guide </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestJavaScriptGOM extends TestCase2 {

    public TestJavaScriptGOM() {
    }

    public TestJavaScriptGOM(String name) {
        super(name);
    }

    protected ScriptEngine jsEngine;
    
    protected void setUp() throws Exception {

        final ScriptEngineManager mgr = new ScriptEngineManager();

//        for (ScriptEngineFactory f : mgr.getEngineFactories()) {
//
//            log.error("Factory: " + f);
//
//        }
        
        jsEngine = mgr.getEngineByName("ECMAScript"); // or JavaScript
        
//        �try {
//        �} catch (ScriptException ex) {
//        � � �ex.printStackTrace();
//        �}
    }

    protected void tearDown() throws Exception {

        jsEngine = null;
        
    }

    /**
     * Verify that the scripting engine is running.
     */
    public void testScriptRuns() throws ScriptException {

        final String[] attrs = new String[] {
                ScriptEngine.ENGINE,
                ScriptEngine.ENGINE_VERSION,
                ScriptEngine.LANGUAGE,
                ScriptEngine.LANGUAGE_VERSION,
        };
        
        if (log.isInfoEnabled()) {

            for (String s : attrs) {

                log.info(s + "=" + jsEngine.get(s));

            }
            
        }

        jsEngine.eval("print('Hello, world!');");

    }

    /**
     * Verify that the scripting engine will throw an exception if there is an
     * error.
     * 
     * @throws ScriptException
     */
    public void testScriptExceptionThrown() throws ScriptException {

        try {

            /*
             * Note: random() should not be resolved, leading to a thrown
             * exception.
             */

            jsEngine.eval("print('Hello, world!'); y=random()/0");

            fail("Expected exception not thrown");
            
        } catch (ScriptException ex) {
            
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
            
        }

    }
    
}
