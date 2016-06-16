
import org.codehaus.jackson.JsonToken

import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import java.util.zip.GZIPInputStream
import com.gmongo.GMongo

@Grapes([
    @Grab(group='org.codehaus.jackson', module='jackson-mapper-asl', version='1.9.13'),
    @Grab(group='com.gmongo', module='gmongo', version='0.9.3'),
    @GrabConfig(systemClassLoader=true)
])
/**
 * This parser treats a file as an input for one month and only uses the newest stats entry of each instanceId.
 */
class JenkinsWeeklyParser {

    def weeklyData

    /**
     * Pass {@link InstanceMetric} for each installation to the given closure.
     */ 

    public void forEachInstance(File file) throws Exception {
        println "parsing $file"
        MongoDBInstance mongodb = new MongoDBInstance();
        def db = mongodb.getDBInstance("GSoC_jenkins");

        JsonFactory jFactory = new org.codehaus.jackson.map.MappingJsonFactory();

        def f = new FileInputStream(file);
        if (file.name.endsWith(".gz")) f = new GZIPInputStream(f)
        JsonParser jParser = jFactory.createJsonParser(f);

        JsonToken current;

        current = jParser.nextToken();
        if (current != JsonToken.START_OBJECT) {
            println("Error: root must be object!");
            return;
        }
        weeklyData = [:]
        while(jParser.nextToken() != JsonToken.END_OBJECT)
        {
            String installId = jParser.getCurrentName();

            def latestWeekTimestamp = [:]

            current = jParser.nextToken();
            def documents = []
            def document = [:]

            if(installId?.size() == 64 || installId?.size() == 128) { // installation hash is 64 chars

                if(current == JsonToken.START_ARRAY){
                    while(jParser.nextToken() != JsonToken.END_ARRAY){
                        // read the record into a tree model,
                        // this moves the parsing position to the end of it
                        JsonNode jNode = jParser.readValueAsTree();
                        // And now we have random access to everything in the object
                        def timestampStr = jNode.get("timestamp").getTextValue() // 11/Oct/2011:05:14:43 -0400
                        Date parsedDate = Date.parse('dd/MMM/yyyy:HH:mm:ss Z', timestampStr)

                        Calendar cal = Calendar.getInstance();
                        cal.setTime(parsedDate);
                        int week = cal.get(Calendar.WEEK_OF_YEAR);
                        int year = cal.get(Calendar.YEAR);
                        String key = Integer.toString(year)+"_"+Integer.toString(week)

                        if(!latestWeekTimestamp.containsKey(key) || parsedDate.after(latestWeekTimestamp[key]))
                        {
                            latestWeekTimestamp[key] = parsedDate
                            jNode.get("plugins").each {
                                String pName = it.get("name").textValue;
                                String pVersion = it.get("version").textValue;
                                if(!pVersion.contains("SNAPSHOT") && !pVersion.contains("***") && !pVersion.contains("beta") )
                                {
        
                                    document.put("plugin_name", pName);
                                    document.put("weeklyData", []);
                                    def weekData = [:]
                                    weekData.put("week",week);
                                    weekData.put("year", year);
                                    weekData.put("versions", []);
                                    def versions = [:]
                                    versions.put("version_number", pVersion);
                                    versions.put("installation_ids", []);
                                    versions["installation_ids"].add(installId);
                                    weekData["versions"].add(versions)
                                    document["weeklyData"].add(weekData);
                                    def q = ["plugin_name" : pName, "weeklyData.week" : week, "weeklyData.year" : year,"weeklyData.0.versions.0.version_number" : pVersion ];
                                    def u = [ $addToSet: ["weeklyData.versions.installation_ids" : [$each : [ installId ]]]];
                                    def result = db.WeeklyInstallations.findAndModify(q, u)
                                    if(result != null) println "done"
                                    else println "no returns"
                                    //def writeResult = db.WeeklyInstallations.update(q,u)
                                    //if(writeResult["nMatched"] == 0) db.WeeklyInstallations.insert(document);
                                    //db.WeeklyInstallations.insert(document);

                                    /*} else {
                                        if(!document[pName].containsKey(key))
                                        {
                                            document[pName].put(key ,[:])
                                            document[pName][key].put(pVersion, [])
                                            document[pName][key][pVersion].add(installId)
                                        }else{
                                            if(!document[pName][key].containsKey (pVersion)) {
                                                document[pName][key].put(pVersion, [])
                                                document[pName][key][pVersion].add(installId)
                                            }else {
                                                if(!document[pName][key][pVersion].contains(installId)) document[pName][key][pVersion].add(installId)
                                            }
                                        }
                                    }*/
                                }

                            }
                        }
                    }
                }
                
            }

        }
        //def json = new groovy.json.JsonBuilder()
        //json.WeeklyInstallations(weeklyData);
        //db.WeeklyInstallations.insert(json);
    

    }
    
    def run(String[] args) {
        if(args.length>0) {
            args.each { name -> forEachInstance(new File(name)) }
        }
    }
}
new JenkinsWeeklyParser().run(args)
