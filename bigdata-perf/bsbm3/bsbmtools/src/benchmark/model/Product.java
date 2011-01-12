package benchmark.model;

import java.util.*;

public class Product extends BSBMResource{
	private int nr;
	private String label;
	private String comment;
	private ProductType productType;
	private int producer;
	private Vector<Integer> features;
	private Integer[] productPropertyNumerical;
	private String[] productPropertyTextual;
	
	public Product(int productNr, String label, String comment, ProductType productType,
				   int producer) {
		this.producer = producer;
		this.nr = productNr;
		this.label = label;
		this.comment = comment;
		this.productType = productType;
	}

	public int getNr() {
		return nr;
	}

	public String getLabel() {
		return label;
	}

	public String getComment() {
		return comment;
	}

	public ProductType getProductType() {
		return productType;
	}

	public int getProducer() {
		return producer;
	}

	public Vector<Integer> getFeatures() {
		return features;
	}

	public void setFeatures(Vector<Integer> features) {
		this.features = features;
	}

	public Integer[] getProductPropertyNumeric() {
		return productPropertyNumerical;
	}

	public void setProductPropertyNumeric(Integer[] productPropertyNumerical) {
		this.productPropertyNumerical = productPropertyNumerical;
	}

	public String[] getProductPropertyTextual() {
		return productPropertyTextual;
	}

	public void setProductPropertyTextual(String[] productPropertyTextual) {
		this.productPropertyTextual = productPropertyTextual;
	}
	
	@Override
    public String toString()
	{
		return getURIref(nr, producer);
	}
	
	public static String getURIref(int productNr, int producerNr)
	{
		StringBuffer s = new StringBuffer();
		s.append("<");
		s.append(Producer.getProducerNS(producerNr));
		s.append("Product");
		s.append(productNr);
		s.append(">");
		return s.toString();
	}
	
	public static String getPrefixed(int productNr, int producerNr) {
		StringBuffer s = new StringBuffer();
		s.append(Producer.getProducerNSprefixed(producerNr));
		s.append("Product");
		s.append(productNr);
		return s.toString();
	}
}
