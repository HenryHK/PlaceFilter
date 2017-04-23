package task3;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input photo-id owner tags data-taken place-id accuracy
// output place-id tags

public class PhotoMapper extends  Mapper<Object, Text, TextIntPair, Text>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		//Split the input data into array
		String[] dataArray = value.toString().split("\t");
		
		//Should be 7
		System.out.println("Array size of splited input data: " + dataArray.length);
		
		//A not complete record with all data
		if (dataArray.length < 6)
		{
			//Don't emit anything
			return; 
		}
			
        String place_id = dataArray[4].trim();
        String tags = dataArray[2].trim();

        context.write(new TextIntPair(place_id, 1), new Text(tags));
		
	}
}