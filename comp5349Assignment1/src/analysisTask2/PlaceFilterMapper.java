package analysisTask2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//This mapper is used to select place_id and place_name out from place.txt
//Steps: 1. select only 7, 2. output place_id and place_name
//Input format: place_id \t woeid \t latitude \t longitude \t place_name \t place_type_id \t place_url
//Output format: place_id \t place_name (only place_type: 7)
//@see PlaceFilterDriver
//@author Zhao Liu

public class PlaceFilterMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text placeId= new Text(), placeName = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		//Split the input data into array
		String[] dataArray = value.toString().split("\t");
		
		//Should be 7
		System.out.println("Array size of splited input data: " + dataArray.length);
		
		//A not complete record with all data
		if (dataArray.length < 7)
		{
			//Don't emit anything
			return; 
		}
		
		//Select 7 only
		if (dataArray[5].trim().equals("7"))
		{
			String place_Id = dataArray[0].trim();
			String place_name = dataArray[4].trim();
			placeId.set(place_Id);
			placeName.set(place_name);
			context.write(placeId, placeName);
		}
		else
		{
			return;
			
		}
		
	}

}
