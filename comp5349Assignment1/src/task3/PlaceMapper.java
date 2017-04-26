package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import task3.TextIntPair;

//input place-id woeid latitude longtitude place-name place-type-id place-url
//output place-id place-name place-type-id

public class PlaceMapper extends Mapper<Object, Text, TextIntPair, Text>{

    //private TextIntPair pairKey= new TextIntPair(), combinedValue = new Text();
	
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
			
        String place_id = dataArray[0].trim();
        String place_name = dataArray[4].trim();
        String place_type_id = dataArray[5].trim();

        context.write(new TextIntPair(place_id, 0), new Text(place_name + "\t" + place_type_id));
		
	}

}