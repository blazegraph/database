/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
package com.bigdata.btree;

import java.util.Locale;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * <p>
 * Helper class for building up variable <code>unsigned byte[]</code> keys
 * from one or more primitive data types values and/or Unicode strings. An
 * instance of this class may be {@link #reset()} and reused to encode a series
 * of keys.
 * </p>
 * <p>
 * This class uses <a href="http://icu.sourceforge.net">ICU4J</a>. There are
 * several advantages to the ICU libraries: (1) the collation keys are
 * compressed; (2) the libraries are faster than the jdk classes; (3) the
 * libraries support Unicode 5; and (4) the libraries have the same behavior
 * under Java and C/C++ so you can have interoperable code. There is also JNI
 * (Java Native Interface) implementation for many platforms for even greater
 * performance and compatibility.
 * </p>
 * 
 * FIXME verify runtime with ICU4JNI, optimize generation of collation keys ( by
 * using lower level iterators over the collation groups), and remove dependency
 * on ICU4J if possible (it should have a lot of stuff that we do not need if we
 * require the JNI integration; alternatively make sure that the code will run
 * against both the ICU4JNI and ICU4J interfaces).
 * 
 * FIXME there appears to be an issue between ICU4JNI and jrockit under win xp
 * professional. try the icu4jni 3.6.1 patch (done, but does not fix the
 * problem) and ask on the mailing list.
 * 
 * FIXME TestAvailableCharsets Charset.availableCharsets() returned a number
 * less than the number returned by icu -- is this a serious error? - it
 * prevents the test suite from completing correctly. check on the mailing list.
 * 
 * FIXME Apply the 3.6.1 patch for ICU4JNI and rebuild the distribution.
 * 
 * @todo try out the ICU boyer-moore search implementation if it is defined for
 *       sort keys not just char[]s.
 * 
 * @todo introduce a mark and restore feature for generating multiple keys that
 *       share some leading prefix. in general, this is as easy as resetting the
 *       len field to the mark. keys with multiple components could benefit from
 *       allowing multiple marks.
 * 
 * @todo cross check index metadata for the correct locale and collator
 *       configuration and version code.
 * 
 * @todo transparent use of ICU4JNI when available.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnicodeKeyBuilder extends KeyBuilder implements IKeyBuilder {

    protected static final RuleBasedCollator assertNotNull(
            RuleBasedCollator collator) {

        if (collator == null)
            throw new IllegalArgumentException("collator");

        return collator;

    }
    
    /**
     * Creates a key builder that will use the
     * {@link Locale#getDefault() default locale} to encode strings and an
     * initial buffer capacity of <code>1024</code> bytes.
     * <p> 
     * Note: The according to the ICU4j documentation, the default strength
     * for the Collator is TERTIARY unless specified by the locale.
     * 
     * @see RuleBasedCollator
     * @see Collator#getInstance(Locale)
     * @see Locale
     */
    public UnicodeKeyBuilder() {
        
        this(DEFAULT_INITIAL_CAPACITY);
        
    }
    
    /**
     * Creates a key builder that will use the
     * {@link Locale#getDefault() default locale} to encode strings and the
     * specified initial buffer capacity.
     * <p>
     * Note: The according to the ICU4j documentation, the default strength for
     * the Collator is TERTIARY unless specified by the locale.
     * 
     * @param initialCapacity
     *            The initial capacity of the internal byte[] used to construct
     *            keys.
     * 
     * @see RuleBasedCollator
     * @see Collator#getInstance(Locale)
     * @see Locale
     */
    public UnicodeKeyBuilder(int initialCapacity) {
        
        this((RuleBasedCollator) Collator.getInstance(Locale.getDefault()),
                initialCapacity);
        
    }
    
    /**
     * Creates a key builder that will use the
     * {@link Locale#getDefault() default locale} to encode strings and the
     * specified initial buffer capacity.
     * <p>
     * Note: The according to the ICU4j documentation, the default strength for
     * the Collator is TERTIARY unless specified by the locale.
     * 
     * @param len
     *            The #of bytes of data in the provided buffer.
     * @param buf
     *            The buffer, with <i>len</i> pre-existing bytes of valid data.
     *            The buffer reference is used directly rather than making a
     *            copy of the data.
     * 
     * @see RuleBasedCollator
     * @see Collator#getInstance(Locale)
     * @see Locale
     */
    public UnicodeKeyBuilder(int len,byte[] buf) {
        
        this((RuleBasedCollator) Collator.getInstance(Locale.getDefault()),
                len, buf);
        
    }

    /**
     * Creates a key builder.
     * 
     * @param collator
     *            The collator used to encode Unicode strings.
     *            
     * @param initialCapacity
     *            The initial capacity of the internal byte[] used to construct
     *            keys.
     * 
     * @see RuleBasedCollator
     * @see Collator#getInstance(Locale)
     * @see Locale
     */
    public UnicodeKeyBuilder(RuleBasedCollator collator, int initialCapacity) {

        this(collator, 0, new byte[assertNonNegative("initialCapacity",
                initialCapacity)]);
        
    }
    
    /**
     * Creates a key builder using an existing buffer with some data (designated
     * constructor).
     * 
     * @param collator
     *            The collator used to encode Unicode strings.
     * @param len
     *            The #of bytes of data in the provided buffer.
     * @param buf
     *            The buffer, with <i>len</i> pre-existing bytes of valid data.
     *            The buffer reference is used directly rather than making a
     *            copy of the data.
     * 
     * @see RuleBasedCollator
     * @see Collator#getInstance(Locale)
     * @see Locale
     */
    public UnicodeKeyBuilder(RuleBasedCollator collator, int len, byte[] buf) {

        super(assertNotNull(collator), len,buf);
        
    }
    
}
