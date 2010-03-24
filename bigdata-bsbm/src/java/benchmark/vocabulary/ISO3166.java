package benchmark.vocabulary;

import java.util.HashMap;

public class ISO3166 {
	
	static final HashMap<String, String> countries = new HashMap<String, String>();
	public static final HashMap<String, Byte> countryCodes = new HashMap<String, Byte>();
	
	public final String code;
	
	private ISO3166(String code) {
        this.code = code.toUpperCase();
        countries.put(this.code, ISO3166.NS + this.code);
	}
	
	static public String find(String countryId) {
        return countries.get(countryId);
    }
	
	//The Namespace of this vocabulary as String
	public static final String NS = "http://downlode.org/rdf/iso-3166/countries#";
	
	//Get the URI of this vocabulary
	public static String getURI() { return NS; }
	
	static
	{
		new ISO3166("US");
		new ISO3166("GB");
		new ISO3166("JP");
		new ISO3166("CN");
		new ISO3166("DE");
		new ISO3166("FR");
		new ISO3166("ES");
		new ISO3166("RU");
		new ISO3166("KR");
		new ISO3166("AT");
	}

	public final static byte US = 1; 
	public final static byte GB = 2; 
	public final static byte JP = 3; 
	public final static byte CN = 4; 
	public final static byte DE = 5; 
	public final static byte FR = 6; 
	public final static byte ES = 7; 
	public final static byte RU = 8; 
	public final static byte KR = 9; 
	public final static byte AT = 10;
	
	static
	{
		countryCodes.put("US", US);
		countryCodes.put("GB", GB);
		countryCodes.put("JP", JP);
		countryCodes.put("CN", CN);
		countryCodes.put("DE", DE);
		countryCodes.put("FR", FR);
		countryCodes.put("ES", ES);
		countryCodes.put("RU", RU);
		countryCodes.put("KR", KR);
		countryCodes.put("AT", AT);
	}
	
	//Not ISO3166, but spoken language in specific countries
	public final static String[] language = new String[11];
	
	static
	{
		language[1] = "en";
		language[2] = "en";
		language[3] = "ja";
		language[4] = "zh";
		language[5] = "de";
		language[6] = "fr";
		language[7] = "es";
		language[8] = "ru";
		language[9] = "ko";
		language[10] = "de";
	}
}
