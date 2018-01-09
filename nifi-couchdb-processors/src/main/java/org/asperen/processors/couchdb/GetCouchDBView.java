package org.asperen.processors.couchdb;

import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public class GetCouchDBView extends AbstractCouchDBView {

	private static final Validator VIEWNAME_VALIDATOR = new Validator() {
		@Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
			String reason = null;
			if (!value.contains("/")) {
				reason = "does not contain /";
			} else {
				String[] parts = value.split("/");
				if (parts.length != 2)
					reason = "does not conform to the required pattern \"designdoc/viewname\"";
				else
					for (int i=0; i < 2 && reason == null; i++)
						if (!parts[i].matches("[a-zA-Z_0-9]+"))
							reason = String.format("part %d contains illegal characters",i);
			}
			
			return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
		}
	};
	
	public static final PropertyDescriptor COUCHDB_VIEWNAME= new PropertyDescriptor
	            .Builder().name("COUCHDB_VIEWNAME")
	            .displayName("View name")
	            .description("The name of the view, in the form \"designdocument/viewname\".")
	            .required(true)
	            .addValidator(VIEWNAME_VALIDATOR)
	            .build();

	@Override
	protected void initDescriptors(List<PropertyDescriptor> descriptors) {
		descriptors.add(COUCHDB_VIEWNAME);
		super.initDescriptors(descriptors);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		String viewName = context.getProperty(COUCHDB_VIEWNAME).getValue();
		retrieveView(context, session, viewName);
	}

}
