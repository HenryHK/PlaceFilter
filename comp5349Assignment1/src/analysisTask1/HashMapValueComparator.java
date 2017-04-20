package analysisTask1;

import java.util.Comparator;
import java.util.Map;

//This comparator is used to sort values (tagsFrequency) in TagsReducer in descending order.
//@author Zhao Liu


public class HashMapValueComparator implements Comparator<String> 
{
	Map<String, Integer> map;
	
	public HashMapValueComparator(Map<String, Integer> map)
	{
		this.map = map;
	}
	
	public int compare(String arg0, String arg1)
	{
		if (!map.containsKey(arg0) || !map.containsKey(arg1))
		{
			return 0;
		}
		if (map.get(arg0) < map.get(arg1))
		{
			return 1;
		}
		else if (map.get(arg0) == map.get(arg1))
		{
			
			if (arg0.compareTo(arg1) > 0)

			{
				return 1;
			}
			else
			{
				return -1;

			}
			
			
		}
		else
		{
			return -1;
		}
		
	}
}
