package org.asperen.processors.couchdb;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.lightcouch.Page;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public abstract class AbstractCouchDBView extends AbstractCouchDB {

	public static final PropertyDescriptor COUCHDB_PAGINATED = new PropertyDescriptor
	            .Builder().name("COUCHDB_PAGINATED")
	            .displayName("Paginated")
	            .description("Indicate if the result should come in pages (defaults to false). Only works for simple string keys.")
	            .required(false)
	            .defaultValue("false")
	            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
	            .build();
	public static final PropertyDescriptor COUCHDB_PAGESIZE = new PropertyDescriptor
	            .Builder().name("COUCHDB_PAGESIZE")
	            .displayName("Page size")
	            .description("The maximum number of rows to load into a single page flow-file (defaults to 100).")
	            .required(false)
	            .defaultValue("100")
	            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
	            .build();

	public AbstractCouchDBView() {
		super();
	}

	@Override
	protected void initDescriptors(List<PropertyDescriptor> descriptors) {
		descriptors.add(COUCHDB_PAGINATED);
		descriptors.add(COUCHDB_PAGESIZE);
		super.initDescriptors(descriptors);
	}

	protected void retrieveView(ProcessContext context, ProcessSession session, String viewName) throws ProcessException {
		createDbClient(context);
		
		try {
			if (context.getProperty(COUCHDB_PAGINATED).asBoolean()) {
				final Gson gson = new GsonBuilder().create();
				int pageSize = context.getProperty(COUCHDB_PAGESIZE).asInteger();
				String nextPage = null;
				do {
					final Page<JsonObject> page = dbClient.view(viewName).queryPage(pageSize, nextPage, JsonObject.class);
					final String jsonPage = gson.toJson(page);
					
					FlowFile flowFile = session.create();
					flowFile = session.putAttribute(flowFile, "filename", viewName + "_" + String.valueOf(page.getPageNumber()));
					flowFile = session.putAttribute(flowFile, "path", dbClient.getDBUri().toString());
					flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
					flowFile = session.write(flowFile, new OutputStreamCallback() {
						
						@Override
						public void process(OutputStream out) throws IOException {
							out.write(jsonPage.getBytes());
						}
						
					});
					session.transfer(flowFile, SUCCESS);
					
					nextPage = page.isHasNext() ? page.getNextParam() : null;
				} while (nextPage != null);
			} else {
				FlowFile flowFile = session.create();
				flowFile = session.putAttribute(flowFile, "filename", viewName);
				flowFile = session.putAttribute(flowFile, "path", this.dbClient.getDBUri().toString());
				flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
				flowFile = session.importFrom(this.dbClient.view(viewName).queryForStream(), flowFile);
				session.transfer(flowFile, SUCCESS);
			} 
		} catch (RuntimeException e) {
			context.yield();
			session.rollback();
			getLogger().error("Failed to retrieve all documents", e);
			throw new ProcessException(e);
		}
		
		session.commit();
	}

}