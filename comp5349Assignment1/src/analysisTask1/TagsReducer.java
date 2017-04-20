package analysisTask1;

import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collections;
import java.util.HashMap;
//import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//This reducer is used to calculate the number of photos and order tags frequency
//Input format: place_name \t tags
//Output format: place_name \t numberOfPhotos \t (tags:freq)+
//@author Zhao Liu

public class TagsReducer extends Reducer<Text, Text, Text, Text> 
{
	private Text result = new Text();
	private Text placeName = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		
		//Create a HashMap list to remember tag frequency
		Map<String, Integer> ptFrequency = new HashMap<String,Integer>();
		int place_Frequency = 0;
		
		//Read each value
		for (Text text: values)
		{
			place_Frequency++;
			String tags = text.toString();
			
			//Split tags from each value 
			String[] tagArray = tags.split(" ");
			
			//Calculate tag frequency
			for (int i = 0; i < tagArray.length; i++)
			{
				if (ptFrequency.containsKey(tagArray[i].trim()))
				{
					ptFrequency.put(tagArray[i].trim(), ptFrequency.get(tagArray[i].trim()) + 1);
					
				}
				else
				{
					ptFrequency.put(tagArray[i].trim(), 1);
				}
			}
			

		}
		
		//Tags sorting		
		HashMapValueComparator sortingList = new HashMapValueComparator(ptFrequency);
//		List<String> keyList=new ArrayList<String>(ptFrequency.keySet());
		for (String mapKey: ptFrequency.keySet())
		{
			System.out.println("Key: " + mapKey);	
		}
//		Collections.sort(keyList, sortingList);
		
		//TreeMap sorting
		TreeMap<String, Integer> sortedList = new TreeMap<String, Integer>(sortingList);
		
		
		//Output results
//		StringBuffer strBuf = new StringBuffer();
//		int top10 = 0;
//		for (String tag: keyList)
//		{
//			if (top10 < 10)
//			{
//				strBuf.append("\t" + "(" + tag + ":" + ptFrequency.get(tag) + ")");
//				top10++;
//			}
//			else
//			{
//				break;
//			}
//
//		}
		
		//Output results for Treemap
		StringBuffer strBuf = new StringBuffer();
		sortedList.putAll(ptFrequency);
		int top10 = 0;
		for (String tag: sortedList.keySet())
		{
			if (top10 < 10)
			{
				strBuf.append("\t" + "(" + tag + ":" + ptFrequency.get(tag) + ")");
				top10++;
			}
			else
			{
				break;
			}

		}
		
		placeName.set(key);
		result.set(place_Frequency + strBuf.toString());
		context.write(placeName, result);

	}

}
