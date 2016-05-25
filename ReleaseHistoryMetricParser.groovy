import org.codehaus.jackson.JsonToken
import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import java.util.zip.GZIPInputStream
@Grapes([
    @Grab(group='org.codehaus.jackson', module='jackson-mapper-asl', version='1.9.13'),
    @GrabConfig(systemClassLoader=true)
])

class ReleaseHistoryParser{

	public void ReleaseHistory(File file) throws Exception {
		println "parsing $file"

		JsonFactory jFactory = new org.codehaus.jackson.map.MappingJsonFactory();

		def f = new FileInputStream(file);
		if(file.name.endsWith(".gz")) f = new GZIPInputStream(f);

		JsonParser jParser = jFactory.createJsonParser(f);

		JsonToken current = jParser.nextToken();
		if (current != JsonToken.START_OBJECT) {
            println("Error: root must be object!");
            return;
        }
        
        def count = 0;
        def releases = [:]
        while (jParser.nextToken() != JsonToken.END_OBJECT) {
        	
        	current = jParser.nextToken();
        	if (current == JsonToken.START_ARRAY) {
        		while (jParser.nextToken() != JsonToken.END_ARRAY) {
        			count++;
        			JsonNode jNode = jParser.readValueAsTree();
        			def date = jNode.get("date").getTextValue();
        			Date parsedDate = Date.parse('MMM dd, yyyy', date)
        			def list = []
        			jNode.get("releases").each{ 
        				def temp_gav = it.get("gav") == null ? "N/A" : it.get("gav").textValue ;
        				def temp_version = it.get("version") == null ? "N/A" : it.get("version").textValue ;
        				def temp_timestamp = it.get("timestamp") == null ? "N/A" : it.get("timestamp").textValue ;
        				def temp_title = it.get("title") == null ? "N/A" : it.get("title").textValue ;
        				def metric = new ReleaseInstance(gav: temp_gav, title: temp_title, timestamp: temp_timestamp, version: temp_version);
        				list.add(metric);
        			}
					releases.put(parsedDate, list);
        		}
        	}
        }

        releases.each { 
        	k, v -> 
        	print "$k : ";
        	print "[  "
        	v.each { print "[$it.gav,  $it.version], " };
        	println " ";
        }


	}

	def run(String[] args) {
		if(args.length>0) {
			args.each { name -> ReleaseHistory(new File(name))}
		}
	}
}

new ReleaseHistoryParser().run(args)