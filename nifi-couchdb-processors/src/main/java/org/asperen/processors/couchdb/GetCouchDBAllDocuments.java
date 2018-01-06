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

public class GetCouchDBAllDocuments extends AbstractCouchDB {
	
	private static final String VIEW_ALL_DOCS = "_all_docs";

	public static final PropertyDescriptor COUCHDB_PAGESIZE = new PropertyDescriptor
            .Builder().name("COUCHDB_PAGESIZE")
            .displayName("Page size")
            .description("The maximum number of rows to load into a single flow-file (defaults to 0 which means 'all').")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

	@Override
	protected void initDescriptors(List<PropertyDescriptor> descriptors) {
		descriptors.add(COUCHDB_PAGESIZE);
		super.initDescriptors(descriptors);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		createDbClient(context);
		
		int pageSize = context.getProperty(COUCHDB_PAGESIZE).asInteger();
		try {
			if (pageSize <= 0) {
				FlowFile flowFile = session.create();
				flowFile = session.putAttribute(flowFile, "filename", VIEW_ALL_DOCS);
				flowFile = session.putAttribute(flowFile, "path", this.dbClient.getDBUri().toString());
				flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
				flowFile = session.importFrom(this.dbClient.view(VIEW_ALL_DOCS).queryForStream(), flowFile);
				session.transfer(flowFile, SUCCESS);
			} else {
				final Gson gson = new GsonBuilder().create();
				String nextPage = null;
				do {
					final Page<JsonObject> page = dbClient.view(VIEW_ALL_DOCS).queryPage(pageSize, nextPage, JsonObject.class);
					final String jsonPage = gson.toJson(page);
					
					FlowFile flowFile = session.create();
					flowFile = session.putAttribute(flowFile, "filename", VIEW_ALL_DOCS + "_" + String.valueOf(page.getPageNumber()));
					flowFile = session.putAttribute(flowFile, "path", this.dbClient.getDBUri().toString());
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
