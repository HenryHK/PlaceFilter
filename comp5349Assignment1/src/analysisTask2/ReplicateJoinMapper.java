package analysisTask2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//This mapper is used to select place_name and owner out from combining the output file of PlaceFilterMapper with photo.txt.
//It uses DistributedCache as input to join with photo.txt
//The files to be distributed are setup in the driver method
//Steps: 
//1. setup method to read the file and store its content as a key value pair of place_id and place_name into hashtable
//2. match two input files through place_id
//3. output place_name and owner
//Input format: place_id \t place_name
//Output format: country_name \t locality \t owner
//@see ReplicateJoinDriver
//@author Zhao Liu

public class ReplicateJoinMapper extends Mapper<Object, Text, Text, Text>
{
	private Hashtable <String, String> placeTable = new Hashtable<String, String>();
	private Text localityAndOwner = new Text(), countryName = new Text();
	
	public void setPlaceTable(Hashtable<String,String> place)
	{
		placeTable = place;
	}
	
	// Get the distributed file and parse it
	public void setup(Context context) throws java.io.IOException, InterruptedException
	{
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0)
		{
			String line;
			String[] tokens;
			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try 
			{
				while ((line = placeReader.readLine()) != null) 
				{
					tokens = line.split("\t");
					//Should be 2
					System.out.println("Array size from intermediate input data for ReplicateJoinMapper: " + tokens.length);
					
					placeTable.put(tokens[0].trim(), tokens[1].trim());
				}
				System.out.println("Size of the place table is: " + placeTable.size());
			} 
			finally 
			{
				placeReader.close();
			}
		}
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		//Split photo.txt into array
		String[] dataArray = value.toString().split("\t");
		
		//Should be 6
		System.out.println("Array size from photo.txt for ReplicateJoinMapper: " + dataArray.length);
		// A not complete record with all data
		if (dataArray.length < 6)
		{
			//Don't emit anything
			return;
		}
		
		
		String place_Id = dataArray[4].trim();
		if (placeTable.containsKey(place_Id))
		{
			System.out.println("PlaceName: " + placeTable.get(place_Id).trim());
			String[] placeArray = placeTable.get(place_Id).split(",");
			
			String country_name = placeArray[placeArray.length - 1].trim();
			countryName.set(country_name);
			System.out.println("CountryName: " + country_name);
			String locality = placeArray[0].trim();
			System.out.println("Locality: " + locality);
			//Get owner
			String owner_id = dataArray[1].trim();
			System.out.println("Owner: " + owner_id);
			localityAndOwner.set(locality + "\t" + owner_id);
			context.write(countryName, localityAndOwner);
		}
		else
		{
			return;
		}
		
	}

}
