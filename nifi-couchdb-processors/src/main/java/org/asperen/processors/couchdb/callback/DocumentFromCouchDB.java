package org.asperen.processors.couchdb.callback;

import java.io.InputStream;

public abstract class DocumentFromCouchDB extends AbstractCouchDBInputStreamCallback {
	protected String id = null;
	protected String rev = null;
	protected InputStream documentStream = null;
	
	public String getId() {
		return id;
	}

	public String getRev() {
		return rev;
	}
	
	public InputStream getDocumentStream() {
		return documentStream;
	}
	
}