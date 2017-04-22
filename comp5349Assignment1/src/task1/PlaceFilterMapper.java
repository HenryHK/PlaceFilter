package task1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//input place.txt
//output PlaceName \t 1

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
		
		//Select 22 and 7
		if (dataArray[5].trim().equals("7"))
		{
			String place_name = dataArray[4].trim();
			placeName.set(place_name);
			context.write(placeName, new Text("1"));
		}
		else if (dataArray[5].trim().equals("22"))
		{
			String place_Id = dataArray[0].trim();
			
			//Cut neighborhood out from 22
			int index = dataArray[4].trim().indexOf(",");
			String place_name = dataArray[4].trim().substring(index + 1).trim();
			
			placeName.set(place_name);
			context.write(placeName, new Text("1"));
			
		}
		else
		{
			return;
			
		}
		
	}

}
