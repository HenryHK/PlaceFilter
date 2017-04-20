package analysisTask2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//This reducer is used to calculate the number of owner and output top 10 of locality-level places depending on NumOfUsers.
//Input format: country_name \t locality \t owner
//Output format: country_name \t (locality:NumOfUsers)+
//@author Zhao Liu

public class OwnerReducer extends Reducer<Text, Text, Text, Text> 
{
	private Text result = new Text();
	private Text countryName = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		
		//Create a HashMap list to remember owner frequency
		Map<String, List<String>> ownerFrequency = new HashMap<String,List<String>>();

		//Read each value
		for (Text text: values)
		{
			String localityAndOwner = text.toString();
			String[] loArray = localityAndOwner.split("\t");
			
			//Should be 2
			System.out.println("loArray size: " + loArray.length);
			String locality = loArray[0].trim();
			System.out.println("Loality: " + locality);
			String owner = loArray[1].trim();
			System.out.println("Owner: " + owner);
			System.out.println("//////////////////////////////////");
			if (ownerFrequency.containsKey(locality))
			{
				if (!ownerFrequency.get(locality).contains(owner))
				{
					ownerFrequency.get(locality).add(owner);
				}
			}
			else
			{
				List<String> ownerList = new LinkedList<String>();
				ownerFrequency.put(locality, ownerList);
				ownerFrequency.get(locality).add(owner);
				
			}

		}
	
		//Owners sorting
		Map<String, Integer> placeAndUserNum = new HashMap<String,Integer>();
		
		for (String locality: ownerFrequency.keySet())
		{
			int totalNum = ownerFrequency.get(locality).size();
			placeAndUserNum.put(locality, totalNum);
		}
		
		//Locality sorting
		HashMapValueComparator sortingList = new HashMapValueComparator(placeAndUserNum);
//		List<String> keyList=new ArrayList<String>(placeAndUserNum.keySet());
		
		//Locality sorting for Treemap
		TreeMap<String, Integer> sortedList = new TreeMap<String, Integer>(sortingList);
		
		for (String locality: placeAndUserNum.keySet())
		{
			System.out.println("Locality: " + locality);	
		}
//		Collections.sort(keyList, sortingList);
		
		//Output final result
//		StringBuffer strBuf = new StringBuffer();
//		int top10 = 0;
//		for (String locality: keyList)
//		{
//			if (top10 < 10)
//			{
//				strBuf.append("\t" + "(" + locality + ":" + placeAndUserNum.get(locality) + ")");
//				top10++;
//			}
//			else
//			{
//				break;
//			}
//
//		}
		
		//Output result for Treemap
		StringBuffer strBuf = new StringBuffer();
		sortedList.putAll(placeAndUserNum);
		int top10 = 0;
		for (String locality: sortedList.keySet())
		{
			if (top10 < 10)
			{
				strBuf.append("\t" + "(" + locality + ":" + placeAndUserNum.get(locality) + ")");
				top10++;
			}
			else
			{
				break;
			}

		}
		
		countryName.set(key);
		result.set(strBuf.toString());
		context.write(countryName, result);

	}

}
