import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.HttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.BigqueryScopes
import com.google.api.services.bigquery.model.TableSchema
import com.google.common.collect.ImmutableList

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.util.Collection

@Grapes([
    @Grab(group='com.google.apis', module='google-api-services-bigquery', version='v2-rev302-1.22.0'),
    @Grab(group='com.google.api-client', module='google-api-client', version='1.22.0'),
    @GrabConfig(systemClassLoader=true)
])

/**
  * @author Payal Priyadarshini
  */

class BigQueryConfig{
	def projectId
	def datasetId
	def tableId
	def bigQuery

	def BigQueryConfig( projectId, datasetId, tableId, credentailFile)
	{
		this.projectId = projectId
		this.datasetId = datasetId
		this.tableId = tableId
		try {
			this.bigQuery = createAuthorizedClient(credentailFile)
		} catch(IOException e) {
			throw new RuntimeException("Failed to authenticate with Google BigQuery: "+e.getMessage(), e);
		}
	}
	public def getProjectId()
	{
		return projectId;
	}
	public def getDatasetId()
	{
		return datasetId;
	}
	public def getTableId()
	{
		return tableId;
	}
	private  Bigquery createAuthorizedClient(File credentialFile) throws IOException {
        // Create the credential
        HttpTransport transport = new NetHttpTransport();
        com.google.api.client.json.JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential=null;
        
        credential = GoogleCredential.fromStream(new FileInputStream(credentialFile));
        if(credential == null){
            throw new RuntimeException("No google credentials found");
        }

        if (credential.createScopedRequired()) {
            Collection<String> bigqueryScopes = ImmutableList.of(BigqueryScopes.BIGQUERY);
            credential = credential.createScoped(bigqueryScopes);
        }

        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("Jenkins Usage Table Access").build();
    }


}