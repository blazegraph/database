package benchmark.model;

import java.util.Locale;

public class Offer extends BSBMResource {
	private int nr;
	private Integer product;
	private int vendor;
	private double price;
	private long validFrom;
	private long validTo;
	private Integer deliveryDays;
	private String offerWebpage;
	
	public Offer(int nr, Integer product, int vendor, double price,
			     long validFrom, long validTo, int deliveryDays,
			     String offerWebpage) {
		this.nr = nr;
		this.product = product;
		this.vendor = vendor;
		this.price = price;
		//this.currency = currency;
		this.validFrom = validFrom;
		this.validTo = validTo;
		this.deliveryDays = deliveryDays;
		this.offerWebpage = offerWebpage;
	}
	
	public int getNr() {
		return nr;
	}

	public void setNr(int nr) {
		this.nr = nr;
	}

	public Integer getProduct() {
		return product;
	}

	public void setProduct(Integer product) {
		this.product = product;
	}

	public int getVendor() {
		return vendor;
	}

	public void setVendor(int vendor) {
		this.vendor = vendor;
	}

	public double getPrice() {
		return price;
	}
	
	public String getPriceString()
	{
		return String.format(Locale.ENGLISH, "%.2f", price);
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public long getValidFrom() {
		return validFrom;
	}

	public void setValidFrom(long validFrom) {
		this.validFrom = validFrom;
	}

	public long getValidTo() {
		return validTo;
	}

	public void setValidTo(long validTo) {
		this.validTo = validTo;
	}

	public Integer getDeliveryDays() {
		return deliveryDays;
	}

	public void setDeliveryDays(Integer deliveryDays) {
		this.deliveryDays = deliveryDays;
	}

	public String getOfferWebpage() {
		return offerWebpage;
	}

	public void setOfferWebpage(String offerWebpage) {
		this.offerWebpage = offerWebpage;
	}
	
	@Override
    public String toString()
	{
		StringBuffer s = new StringBuffer();
		s.append("<");
		s.append(Vendor.getVendorNS(vendor));
		s.append("Offer");
		s.append(nr);
		s.append(">");
		return s.toString();
	}
	
	public String getPrefixed() {
		StringBuffer s = new StringBuffer();
		s.append(Vendor.getVendorNSprefixed(vendor));
		s.append("Offer");
		s.append(nr);
		return s.toString();
	}
	
	public static String getURIref(int offerNr, int vendorNr)
	{
		StringBuffer s = new StringBuffer();
		s.append("<");
		s.append(Vendor.getVendorNS(vendorNr));
		s.append("Offer");
		s.append(offerNr);
		s.append(">");
		return s.toString();
	}
}
