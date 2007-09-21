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
