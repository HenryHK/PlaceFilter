package analysisTask1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//This mapper is used to exchange location between place_name and numberOfPhotos from the output of TagsReducer
//Input format: place_name \t numberOfPhotos \t (tags:freq)+
//Output format: numberOfPhotos \t place_name \t (tags:freq)+
//@see KeyChangerDriver
//@author Zhao Liu

public class KeyChangerMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text valueOut = new Text(), keyOut = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		//Split the input data into array
		String[] dataArray = value.toString().split("\t");
		
		//Should be 7
		System.out.println("Array size of splited input data: " + dataArray.length);
		
		//A not complete record with all data
//		if (dataArray.length < 7)
//		{
//			//Don't emit anything
//			return; 
//		}
		
		//Exchange location between place_name and numberOfPhotos
		String photoFrequency = dataArray[1].trim();
		String placeName = dataArray[0].trim();
		String tagsFrequency = "";
		
		keyOut.set(photoFrequency);
		
		for (int i = 2; i < dataArray.length; i++)
		{
			tagsFrequency += dataArray[i].trim() + "\t";
		}
		
		valueOut.set(placeName + "\t" + tagsFrequency.trim());
		context.write(keyOut, valueOut);	
	}

}
