package task3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//input place-id tags place-name place-type-id 
//output PlaceName \t 1 \t tags

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
		if (dataArray.length < 4)
		{
			//Don't emit anything
			return; 
		}
		
		String tag = dataArray[1];
		
		//tag.replace(oldChar, newChar);

		//Select 22 and 7
		if (dataArray[3].trim().equals("7"))
		{
			String place_name = dataArray[2].trim();
			placeName.set(place_name);
			
			String[] subNames = place_name.trim().split(",");
			for(String subName : subNames){
				tag = tag.replace(" "+subName.toLowerCase()+" ", " ");
				tag = tag.replace(" "+subName.toUpperCase()+" ", " ");
				tag = tag.replace(" "+subName+" ", " ");
				tag = tag.replace(subName.toLowerCase()+" ", "");
				tag = tag.replace(subName.toUpperCase()+" ", "");
				tag = tag.replace(subName+" ", "");
				tag = tag.replace(" "+subName.toLowerCase(), "");
				tag = tag.replace(" "+subName.toUpperCase(), "");
				tag = tag.replace(" "+subName, "");
			}

			context.write(placeName, new Text("1\t" + tag));
		}
		else if (dataArray[3].trim().equals("22"))
		{		
			//Cut neighborhood out from 22
			int index = dataArray[2].trim().indexOf(",");
			String place_name = dataArray[2].trim().substring(index + 1).trim();
			
			String[] subNames = place_name.trim().split(",");
			for(String subName : subNames){
				tag = tag.replace(" "+subName.toLowerCase()+" ", " ");
				tag = tag.replace(" "+subName.toUpperCase()+" ", " ");
				tag = tag.replace(" "+subName+" ", " ");
				tag = tag.replace(subName.toLowerCase()+" ", "");
				tag = tag.replace(subName.toUpperCase()+" ", "");
				tag = tag.replace(subName+" ", "");
				tag = tag.replace(" "+subName.toLowerCase(), "");
				tag = tag.replace(" "+subName.toUpperCase(), "");
				tag = tag.replace(" "+subName, "");
			}

			placeName.set(place_name);
			context.write(placeName, new Text("1\t" + tag));
			
		}
		else
		{
			return;
			
		}
		
	}

}
