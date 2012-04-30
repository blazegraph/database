package com.bigdata.perf.chem2bio2rdf;
import com.bigdata.rdf.vocab.BaseVocabulary;

public class CustomRDFVocabulary extends BaseVocabulary {

	public CustomRDFVocabulary() {
	}

	public CustomRDFVocabulary(String namespace) {
		super(namespace);
	}

	@Override
	protected void addValues() {
		add(new MyVocabularyDecl());
	}

}
