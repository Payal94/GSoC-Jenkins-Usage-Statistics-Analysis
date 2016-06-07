import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException
import com.google.api.client.json.GenericJson
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.BigqueryRequest
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults
import com.google.api.services.bigquery.model.GetQueryResultsResponse
import com.google.api.services.bigquery.model.Job
import com.google.api.services.bigquery.model.QueryRequest
import com.google.api.services.bigquery.model.QueryResponse
import com.google.api.services.bigquery.model.Table
import com.google.api.services.bigquery.model.TableList
import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableSchema
import com.google.api.services.bigquery.model.TableCell
import com.google.api.services.bigquery.model.TableRow

import groovy.xml.MarkupBuilder

import java.io.File
import java.io.IOException
import java.util.Iterator

@Grapes([
    @Grab(group='com.google.apis', module='google-api-services-bigquery', version='v2-rev302-1.22.0'),
    @Grab(group='com.google.api-client', module='google-api-client', version='1.22.0'),
    @GrabConfig(systemClassLoader=true)
])

/**
  * @author Payal Priyadarshini
  */

class PluginVersion {
	
	BigQueryConfig config
	def workingDir = new File("target")
    def svgDir = new File(workingDir, "svg")

    def version_count = [:] 
    int total = 0

	PluginVersion() {
		println "default constructor"
	}

	PluginVersion(projectId, datasetId, tableId, credentialFile)
	{
		try{
			config = new BigQueryConfig(projectId, datasetId, tableId, credentialFile)
			if(config != null) println "credential accepted"
		} catch (IOException e) {
            throw new RuntimeException("Failed to authenticate with Google BigQuery: "+e.getMessage(), e);
        }
	}

	public void executeQuery(String querySql, Bigquery bigquery, String projectId, def svgFile, int waitTime, int year) throws IOException {
        QueryResponse query = config.getBigQuery().jobs().query( projectId, new QueryRequest().setTimeoutMs(waitTime).setQuery(querySql)).execute();
        GetQueryResults queryResult = bigquery.jobs().getQueryResults(
            query.getJobReference().getProjectId(),
            query.getJobReference().getJobId());
        Iterator<GetQueryResultsResponse> pages = getPages(queryResult);
        while(pages.hasNext())
        {
        	generate_stats(pages.next().getRows(), svgFile);
        }
        PieGenerationPreparation(year, svgFile)
    }
    public void generate_stats(List<TableRow> rows, def svgFile) {
    	
    	for(TableRow row : rows) {
    		def fields  = row.getF();
    		TableCell install = fields[0]
    		TableCell time = fields[1]
    		TableCell pVersion = fields[2]

    		if(version_count[pVersion.getV()] == null) version_count[pVersion.getV()] = 1
    		else version_count[pVersion.getV()] = version_count[pVersion.getV()] + 1
    		total = total + 1;

    	}
    	println "total : $total"
    }
    public void PieGenerationPreparation(int year, def svgDir)
    {
    	Map sortedVCount = version_count.sort{ a, b -> b.value <=> a.value }
    	def vCount = []
    	def vName = []
    	int k = 0;
    	int count = 0;
    	sortedVCount.find {
    		version, cnt ->
    		k = k+1
    		if(k == 20) return true
    		vName.add(version)
    		vCount.add(cnt)
    		count = count+cnt
    		println "here $k"
    	}
    	vName.add("others")
    	vCount.add(total - count)
    	vCount.each{ println "$it" }
    	vName.each { println "$it" }
    	createPieSVG("PluginVersion "+year, new File(svgDir, "pluginVersionPieChart_"+year+".svg"), vCount, 200, 300, 150, Helper.COLORS, vName, 370, 20)
    }
    /**
	   * Print rows to the output stream in a formatted way.
	   * @param rows rows in bigquery
	   * @param out Output stream we want to print to
	   */
	 // [START print_rows]
    public void printResults(List<TableRow> rows) {
        System.out.print("\nQuery Results:\n------------\n");
        for (TableRow row : rows) {
          for (TableCell field : row.getF()) {
            System.out.printf("%-50s   ", field.getV());
          }
          System.out.println();
        }
    }

    public  Iterator<GetQueryResultsResponse> getPages(
      BigqueryRequest<GetQueryResultsResponse> requestTemplate) {

	    PageIterator pageIterator =  new PageIterator(requestTemplate);
	    return pageIterator
	  }

    def createPieSVG(def title, def svgFile, def data,def cx,def cy,def r,def colors,def labels,def lx,def ly) {

        // Add up the data values so we know how big the pie is
        def total = 0;
        for(def i = 0; i < data.size(); i++) total += data[i];

        // Now figure out how big each slice of pie is.  Angles in radians.
        def angles = []
        for(def i = 0; i < data.size(); i++) angles[i] = data[i]/total*Math.PI*2;

        // Loop through each slice of pie.
        def startangle = 0;

        def squareHeight = 30

        def viewWidth = lx + 350 // 350 for the text of the legend
        def viewHeight = ly + (data.size() * squareHeight) + 30 // 30 to get some space at the bottom
        def pwriter = new FileWriter(svgFile)
        def pxml = new MarkupBuilder(pwriter)
        pxml.svg('xmlns': 'http://www.w3.org/2000/svg', "version": "1.1", "preserveAspectRatio":'xMidYMid meet', "viewBox": "0 0 $viewWidth $viewHeight") {


            text("x": 30, // Position the text
                    "y": 40,
                    "font-family": "sans-serif",
                    "font-size": "16",
                    "$title, total: $total"){}


            data.eachWithIndex { item, i ->
                // This is where the wedge ends
                def endangle = startangle + angles[i];

                // Compute the two points where our wedge intersects the circle
                // These formulas are chosen so that an angle of 0 is at 12 o'clock
                // and positive angles increase clockwise.
                def x1 = cx + r * Math.sin(startangle);
                def y1 = cy - r * Math.cos(startangle);
                def x2 = cx + r * Math.sin(endangle);
                def y2 = cy - r * Math.cos(endangle);

                // This is a flag for angles larger than than a half circle
                def big = 0;
                if (endangle - startangle > Math.PI) {big = 1}

                // We describe a wedge with an <svg:path> element
                // Notice that we create this with createElementNS()
                //            def path = document.createElementNS(SVG.ns, "path");

                // This string holds the path details
                def d = "M " + cx + "," + cy +      // Start at circle center
                        " L " + x1 + "," + y1 +     // Draw line to (x1,y1)
                        " A " + r + "," + r +       // Draw an arc of radius r
                        " 0 " + big + " 1 " +       // Arc details...
                        x2 + "," + y2 +             // Arc goes to to (x2,y2)
                        " Z";                       // Close path back to (cx,cy)

                path(   d: d, // Set this path
                        fill: colors[i], // Set wedge color
                        stroke: "black", // Outline wedge in black
                        "stroke-width": "1" // 1 unit thick
                        ){}

                // The next wedge begins where this one ends
                startangle = endangle;

                // Now draw a little matching square for the key
                rect(   x: lx,  // Position the square
                        y: ly + squareHeight*i,
                        "width": 20, // Size the square
                        "height": squareHeight,
                        "fill": colors[i], // Same fill color as wedge
                        "stroke": "black", // Same outline, too.
                        "stroke-width": "1"){}

                // And add a label to the right of the rectangle
                text(   "x": lx + 30, // Position the text
                        "y": ly + squareHeight*i + 18,
                        "font-family": "sans-serif",
                        "font-size": "16",
                        "${labels[i]} ($item)"){}
            }
        }
    }
    
    

	def run(String[] args) 
	{
		svgDir.deleteDir()
        svgDir.mkdirs()

		def cli = new CliBuilder(usage: 'Generate Statistics')
		// Location of google credentails file to get the access to the BigQuery APIs.
        cli.credentials(args: 1, argName: 'credentials', "Give credential file", required: true)

        // ID of the project created on the BigQuery.
	    cli.projectId(args: 1, argName: 'projectId', "Give BigQuery Project Id", required: true)

	    // ID of the datset under the above project on BigQuery platform.
	    cli.datasetId(args: 1, argName: 'datasetId', 'Give dataset ID', required: true)

	    // ID of the table need to be accessed under the above dataset.
	    cli.tableId(args: 1, argName: 'tableId', "Give Table ID", required: true)

	    // Plugin name for which stats needs to be generated.
	    cli.plugin(args:1, argName: 'pluginName', "Give plugin name for stats generation", required: true)
	    
	    // Year for which plugin version status needs to be considered.
	    cli.year(args:1, argName: 'year', "Give year for the stats generation", required: true)

	    def options = cli.parse(args)
	    options || System.exit(1)

	    String projectId = options.projectId;
        String datasetId = options.datasetId;
        String tableId = options.tableId;
        String plugin_name = options.plugin;
        int year = Integer.parseInt(options.year);

        def cfile = new File(options.credentials)
        cfile.exists() || exitWithMessage("File not found ${cfile} ")

        try {

            PluginVersion plugin_version_stats = new PluginVersion(projectId, datasetId, tableId, cfile);
    		if(plugin_version_stats == null) println "plugin object not created!"
            
            println "$plugin_name    $year"
            String query = "SELECT install, min(DATE(timestamp)), plugins.version FROM [jenkinsstats.jenkins_usage] where plugins.name = '"+plugin_name+"' and YEAR(timestamp) = "+year+" group by install, plugins.version"
            plugin_version_stats.executeQuery(query, plugin_version_stats.config.getBigQuery(), projectId, svgDir, 10000, year)

        } catch (IOException e) {
            throw new RuntimeException("Failed to authenticate with Google BigQuery: "+e.getMessage(), svgDir);
        }
	}

}
new PluginVersion().run(args)
