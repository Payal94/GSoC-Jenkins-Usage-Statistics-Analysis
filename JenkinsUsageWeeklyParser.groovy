
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

            def pluginCache = [:]

            current = jParser.nextToken();

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
                         //if(!latestStatsDate || parsedDate.after(latestStatsDate)){

                        jNode.get("plugins").each { 
                            String pName = it.get("name").textValue;
                            String pVersion = it.get("version").textValue;
                            if(!pVersion.contains("SNAPSHOT") && !pVersion.contains("***") && !pVersion.contains("beta") )
                            {
                               if(!weeklyData.containsKey(key))
                                {
                                    weeklyData.put(key,[:])
                                    weeklyData[key].put(pName, [:])
                                    weeklyData[key][pName].put(pVersion, [])
                                    weeklyData[key][pName][pVersion].add(installId)
                                } else {
                                    if(!weeklyData[key].containsKey(pName))
                                    {
                                        weeklyData[key].put(pName ,[:])
                                        weeklyData[key][pName].put(pVersion, [])
                                        weeklyData[key][pName][pVersion].add(installId)
                                    }else{
                                        if(!weeklyData[key][pName].containsKey(pVersion)) {
                                            weeklyData[key][pName].put(pVersion, [])
                                            weeklyData[key][pName][pVersion].add(installId)
                                        }else {
                                            if(!weeklyData[key][pName][pVersion].contains(installId)) weeklyData[key][pName][pVersion].add(installId)
                                        }
                                    }
                                }  
                            }   
                        }
                    }
                }
            }

        }
        def json = new groovy.json.JsonBuilder()
        json.WeeklyInstallations(weeklyData);
        db.WeeklyInstallations.insert(json);
    

    }
    
    def run(String[] args) {
        if(args.length>0) {
            args.each { name -> forEachInstance(new File(name)) }
        }
    }
}
new JenkinsWeeklyParser().run(args)
