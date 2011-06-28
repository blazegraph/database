package benchmark.model;

import benchmark.vocabulary.*;

public class Vendor extends BSBMResource {
	private int nr;
	private String label;
	private String comment;
	private String homepage;
	private String countryCode;
	
	public Vendor(int nr, String label, String comment, String homepage, String countryCode)
	{
		this.nr 	 = nr;
		this.label 	 = label;
		this.comment = comment;
		this.homepage = homepage;
		this.countryCode = countryCode;
	}

	public int getNr() {
		return nr;
	}

	public void setNr(int nr) {
		this.nr = nr;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getHomepage() {
		return homepage;
	}

	public void setHomepage(String homepage) {
		this.homepage = homepage;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	
	@Override
    public String toString()
	{
		return getURIref(nr);
	}
	
	public static String getURIref(int vendorNr)
	{
		StringBuffer s = new StringBuffer();
		s.append("<");
		s.append(getVendorNS(vendorNr));
		s.append("Vendor");
		s.append(vendorNr);
		s.append(">");
		return s.toString();
	}
	
	public static String getVendorNS(int vendorNr)
	{
		StringBuffer s = new StringBuffer();
		s.append(BSBM.INST_NS);
		s.append("dataFromVendor");
		s.append(vendorNr);
		s.append("/");
		return s.toString();
	}
	
	public static String getVendorNSprefixed(int vendorNr)
	{
		StringBuffer s = new StringBuffer();
		s.append("dataFromVendor");
		s.append(vendorNr);
		s.append(":");
		return s.toString();
	}
	
	public static String getPrefixed(int vendorNr)
	{
		StringBuffer s = new StringBuffer();
		s.append(getVendorNSprefixed(vendorNr));
		s.append("Vendor");
		s.append(vendorNr);
		return s.toString();
	}
}
