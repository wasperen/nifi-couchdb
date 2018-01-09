package org.asperen.processors.couchdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.asperen.processors.couchdb.callback.AbstractCouchDBInputStreamCallback;
import org.lightcouch.CouchDbException;
import org.lightcouch.DocumentConflictException;
import org.lightcouch.Response;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

@CapabilityDescription("Puts a document to a CouchDB. If the property \"Include document\" is set to true, the resulting document is fetched "
		+ "from the database after storing and sent out on the SUCCESS relationship. If that property is false (the default), only the documents' "
		+ "_id and _rev are sent out on relationship SUCCESS.")
public class PutCouchDBDocument extends AbstractCouchDBDocument {
	
	public static final Relationship CONFLICT = new Relationship.Builder()
            .name("conflict")
            .description("Outputs the FlowFiles that result in a document conflict when stored.")
            .build();

	private class SaveToCouchDB extends AbstractCouchDBInputStreamCallback {

		private Response response = null;

		public Response getResponse() {
			return response;
		}
		
		@Override
		public void process(InputStream in) throws IOException {
			JsonObject inObject = inputStreamToJson(in);
			in.close();
			
			try {
				response = PutCouchDBDocument.this.dbClient.save(inObject);
			} catch (CouchDbException e) {
				exception = e;
				response = null;
			}
		}
		
	}

	@Override
	protected void initRelationships(Set<Relationship> relationships) {
		relationships.add(CONFLICT);
		super.initRelationships(relationships);
	}
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		FlowFile flowFile = session.get();
		if (flowFile == null)
			return;
		
		if (!createDbClientOrFail(context, session, flowFile))
			return;

		final SaveToCouchDB save = new SaveToCouchDB();
		session.read(flowFile, save);
		
		if (save.getException() != null) {
			if (save.getException().getClass() == DocumentConflictException.class) {
				session.transfer(flowFile, CONFLICT);
				return;
			} else
				throw new ProcessException("Error saving document", save.getException());
		}

		if (save.getResponse() != null && save.getResponse().getError() != null) {
			getLogger().warn(String.format("Error saving document: %s; reason: %s", save.getResponse().getError(), save.getResponse().getReason()));
			session.rollback();
			
			session.transfer(flowFile, FAILURE);
			return;
		}

		// only re-obtain the saved object if the output relationship is not auto-terminated
		if (SUCCESS.isAutoTerminated()) {
			session.remove(flowFile);
			return;
		}
		
		if (context.getProperty(COUCHDB_INCLUDE_DOCUMENT).asBoolean()) {
			InputStream objectStream = dbClient.find(save.getResponse().getId(), save.getResponse().getRev());
			session.importFrom(objectStream, flowFile);
			try {
				objectStream.close();
			} catch (IOException e) {
				throw new ProcessException(e);
			}
		} else {
			session.write(flowFile, new OutputStreamCallback() {
				
				@Override
				public void process(OutputStream out) throws IOException {
					Gson gson = new Gson();
					String json = gson.toJson(save.getResponse());
					out.write(json.getBytes());
				}
			});
		}
		flowFile = session.putAttribute(flowFile, "filename", save.getResponse().getId() + "@" + save.getResponse().getRev());
		flowFile = session.putAttribute(flowFile, "path", dbClient.getDBUri().toString());
		flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
		session.transfer(flowFile, SUCCESS);
	}

}
