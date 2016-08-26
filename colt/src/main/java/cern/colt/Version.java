/*
Copyright (c) 1999 CERN - European Organization for Nuclear Research.
Permission to use, copy, modify, distribute and sell this software and its documentation for any purpose 
is hereby granted without fee, provided that the above copyright notice appear in all copies and 
that both that copyright notice and this permission notice appear in supporting documentation. 
CERN makes no representations about the suitability of this software for any purpose. 
It is provided "as is" without expressed or implied warranty.
*/
package cern.colt;

/**
 * Information about the current release.
 * Use this class to distinguish releases and base runtime decisions upon.
 * Versions are of the form <tt>Major.Minor.Micro.Build</tt>, e.g. <tt>1.0.0.52</tt>
 * <p>
 * You can most easily display version info by running <tt>java cern.colt.Version</tt>.
 */
public final class Version {
/**
 * Not yet commented.
 */
private Version() {
}
/**
 * Returns all version information as string.
 */
public static String asString() {
	if (getPackage()==null) return "whoschek@lbl.gov";
	String vendor = getPackage().getImplementationVendor();
	if (vendor==null) vendor = "whoschek@lbl.gov";
	return
		"Version " + 
		getMajorVersion()  + "." +
		getMinorVersion() + "." +
		getMicroVersion()  + "." +
		getBuildVersion() + " (" +
		getBuildTime() + ")" +
		"\nPlease report problems to "+ vendor;
	}
/**
 * Returns the time this release was build; for example "Tue Apr 11 11:50:39 CEST 2000".
 */
public static String getBuildTime() {
	//String s = "1.2.3.56 (Tue Apr 11 11:50:39 CEST 2000)";
	if (getPackage()==null) return "unknown";
	String s = getPackage().getImplementationVersion();
	if (s==null) return "unknown";
	int k = s.indexOf('(');
	return s.substring(k+1,s.length()-1);
}
/**
 * Returns the build version of this release.
 */
public static int getBuildVersion() {
	return numbers()[3];
}
/**
 * Returns the major version of this release.
 */
public static int getMajorVersion() {
	return numbers()[0];
}
/**
 * Returns the micro version of this release.
 */
public static int getMicroVersion() {
	return numbers()[2];
}
/**
 * Returns the minor version of this release.
 */
public static int getMinorVersion() {
	return numbers()[1];
}
/**
 * 
 */
private static Package getPackage() {
	return Package.getPackage("cern.colt");
}
/**
 * Prints <tt>asString</tt> on <tt>System.out</tt>.
 * @param args ignored.
 */
public static void main(String[] args) {
	System.out.println(asString());
}
/**
 * Returns the major version of this release; for example version 1.2.3 returns 1.
 */
private static int[] numbers() {
	int w = 4;
	//int[] numbers = new int[w];
	int[] numbers = new int[] {1, 1, 0, 0};
	return numbers;
	/*
	if (getPackage()==null) return numbers;
	String s = getPackage().getImplementationVersion();
	if (s==null) return numbers;
	int k = s.indexOf('(');
	s = s.substring(0,k);
	s = s.trim();
	//s = s.replace('.', ' ');
	//s = ViolinStrings.Strings.stripBlanks(s);
	//s = ViolinStrings.Strings.translate(s, ".", " ");
	String[] words = s.split("."); // requires jdk 1.4.x
	for (int i=0; i<w; i++) {
		numbers[i] = Integer.parseInt(words[i]);
		//numbers[i] = Integer.parseInt(ViolinStrings.Strings.word(s, i));
		//System.out.println(numbers[i]);
	}
	return numbers;
	*/
}
}
