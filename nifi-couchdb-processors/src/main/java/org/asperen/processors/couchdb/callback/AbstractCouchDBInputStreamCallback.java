package org.asperen.processors.couchdb.callback;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.nifi.processor.io.InputStreamCallback;
import org.lightcouch.CouchDbException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public abstract class AbstractCouchDBInputStreamCallback implements InputStreamCallback {
	
	protected CouchDbException exception = null;
	protected Gson gson = new Gson();
	
	public CouchDbException getException() {
		return exception;
	}

	protected JsonObject inputStreamToJson(InputStream in) {
		Reader reader = new BufferedReader(new InputStreamReader(in));
		return gson.fromJson(reader, JsonObject.class);
	}

}
