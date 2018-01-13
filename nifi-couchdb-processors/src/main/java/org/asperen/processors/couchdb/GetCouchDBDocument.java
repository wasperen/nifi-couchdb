package org.asperen.processors.couchdb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.asperen.processors.couchdb.callback.DocumentFromCouchDB;
import org.lightcouch.NoDocumentException;

import com.google.gson.JsonObject;

@Tags({"couchdb", "ingres"})
@CapabilityDescription("Retrieves a document from a CouchDB database. Expects the content of a Flow File to contain"
		+ " a JSON document description like {\"id\":\"some id\"} or, to retrieve a specific revision:"
				+ "{\"id\": \"some id\", \"rev\":\"some revision\"}. Only when the property \"Include document\" is "
				+ "set to true, is the full document retrieved. If not (the default), only an object containing the "
				+ "documents' id and most recently stored rev are returned.")
public class GetCouchDBDocument extends AbstractCouchDBDocument {

	private static String DOC_ID = "id";
	private static String DOC_REV = "rev";

	public static final Relationship NO_DOCUMENT = new Relationship.Builder()
            .name("no_document")
            .description("Outputs the FlowFiles that refer to a non-existing document.")
            .build();
	
	private class HeadOnlyFromCouchDB extends DocumentFromCouchDB {

		@Override
		public void process(InputStream in) throws IOException {
			JsonObject object = inputStreamToJson(in);
			in.close();
			
			if (object.has(DOC_ID)) {
				this.id = object.get(DOC_ID).getAsString();
				try {
					HttpHead head = new HttpHead(dbClient.getDBUri() + this.id);
					HttpResponse response = dbClient.executeRequest(head);
					this.rev = response.getFirstHeader("ETAG").getValue();
					HttpClientUtils.closeQuietly(response); 
					
					object.addProperty("rev", this.rev);
					this.documentStream = new ByteArrayInputStream(gson.toJson(object).getBytes());
				} catch (NoDocumentException e) {
					this.exception = e;
				}
			} else
				throw new IOException("Not specified document id in retrieval");
		}
		
	}
	
	private class RetrieveFromCouchDB extends DocumentFromCouchDB {

		@Override
		public void process(InputStream in) throws IOException {
			JsonObject object = inputStreamToJson(in);
			in.close();
			
			if (object.has(DOC_ID)) {
				this.id = object.get(DOC_ID).getAsString();
				try {
					if (object.has(DOC_REV)) {
						this.rev = object.get(DOC_REV).getAsString();
						this.documentStream = GetCouchDBDocument.this.dbClient.find(this.id, this.rev);
					} else {
						this.documentStream = GetCouchDBDocument.this.dbClient.find(this.id);
					}
				} catch (NoDocumentException e) {
					this.exception = e;
				}
			} else
				throw new IOException("Not specified document id in retrieval request");
		}

	}
	
	@Override
	protected void initRelationships(Set<Relationship> relationships) {
		relationships.add(NO_DOCUMENT);
		super.initRelationships(relationships);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null)
			return;
		
		if (!createDbClientOrFail(context, session, flowFile))
			return;

		DocumentFromCouchDB load = context.getProperty(COUCHDB_INCLUDE_DOCUMENT).asBoolean() ? new RetrieveFromCouchDB() : new HeadOnlyFromCouchDB();
		session.read(flowFile, load);
		
		if (load.getException() != null) {
			if (load.getException().getClass() == NoDocumentException.class) {
				session.transfer(flowFile, NO_DOCUMENT);
				return;
			} else
				throw new ProcessException(load.getException());
		}
		
		flowFile = session.importFrom(load.getDocumentStream(), flowFile);
		try {
			load.getDocumentStream().close();
		} catch (IOException e) {
			throw new ProcessException(e);
		}
		flowFile = setCoreAttributes(session, flowFile, load.getId() + (load.getRev() != null ? "@" + load.getRev() : ""));
		session.transfer(flowFile, SUCCESS);
	}

}
