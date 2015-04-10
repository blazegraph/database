/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
/*
 * Created on Jun 26, 2006
 */
package com.bigdata.jini.util;

import java.net.MalformedURLException;

import junit.framework.TestCase2;
import net.jini.core.discovery.LookupLocator;

public class TestConfigMath extends TestCase2 {

    public TestConfigMath() {
    }

    public TestConfigMath(final String name) {
        super(name);
    }

    public void test_getLocators() throws MalformedURLException {

        assertSameArray(
                new LookupLocator[] {//
                        new LookupLocator("jini://bigdata15/"),//
                        new LookupLocator("jini://bigdata16/"),//
                        new LookupLocator("jini://bigdata17/"),//
                },
                ConfigMath
                        .getLocators("jini://bigdata15/,jini://bigdata16/,jini://bigdata17/"));

    }

    public void test_getLocators_empty() throws MalformedURLException {

        assertSameArray(new LookupLocator[] {//
                }, ConfigMath.getLocators(""));

    }

    public void test_getLocators_null_arg() throws MalformedURLException {

        try {

            ConfigMath.getLocators(null/* locators */);
            
            fail("Expecting " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            // ignore expected exception
            
        }

    }

    public void test_getGroups1() throws MalformedURLException {

        assertSameArray(new String[] { "a" },
                ConfigMath.getGroups("a"));

    }

    public void test_getGroups3() throws MalformedURLException {

        assertSameArray(new String[] { "a", "b", "c" },
                ConfigMath.getGroups("a,b,c"));

    }

    public void test_getGroups_empty() {

        assertSameArray(new String[] {}, ConfigMath.getGroups(""));

    }

    public void test_getGroups_null_label() {

        assertEquals(null, ConfigMath.getGroups("null"));

    }

}
