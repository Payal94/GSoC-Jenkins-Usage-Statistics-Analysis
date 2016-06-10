@Grab(group='com.gmongo', module='gmongo', version='0.9.3') 
import com.gmongo.GMongo

class MongoDBInstance{
	def mongoClient
	def db
	def MongoDBInstance()
	{
		// Instantiating gmongo.GMongo client, and establishing the connection.
		mongoClient = new GMongo("localhost", 27017)
		
		
	}
	def getDBInstance(String collectionName)
	{
		// Get the object of "GSoC_Jenkins" collection, if it does not exist create it.
		db =  mongoClient.getDB(collectionName)
		if(db == null) println "Error in creating database"
		return db;
	}
	/*def run(String[] args)
	{
		if(args.length>0) {
            args.each { name -> forEachInstance(name) }
        }
		int count = db.jenkins_weekly_data.count()
		db.jenkins_weekly_data.insert([first:"Charles",last:"Darwin"]) 
		println count
	}*/
}
//new MongoDB().run(args)