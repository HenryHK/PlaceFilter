package analysisTask1.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import analysisTask1.TagsReducer;

//A unit test for PlaceAndTagsReducer
//It contains a single test method to test the valid result of photos and tags frequency using the output of mappers
//@author Zhao Liu

public class TagsReducerTest 
{
	@Test
	public void processValidRecord() throws IOException
	{
		
		TagsReducer reducer = new TagsReducer();
		
		Text key = new Text("PlaceName1");
		Text place = new Text();
		ArrayList<Text> values = new ArrayList<Text>();
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag6"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag5 tag7"));
		values.add(new Text("tag1 tag2 tag3 tag4 tag8 tag7"));
		values.add(new Text("tag1 tag2 tag3 tag9 tag8 tag7"));
		values.add(new Text("tag1 tag2 tag10 tag9 tag8 tag7"));
		values.add(new Text("tag1 tag11 tag10 tag9 tag8 tag7"));
		
		Iterable<Text> iterableValue = values;
		Reducer.Context mock = mock(Reducer.Context.class);
		Reducer<Text, Text, Text, Text>.Context context = mock;
		try{
		reducer.reduce(key, iterableValue, context);
		place.set(key);
		verify(context).write(place, new Text("16" + "\t" + "(tag1:16)" + "\t" + "(tag2:15)" + "\t" + "(tag3:14)"  + "\t" + "(tag4:13)" + "\t" + "(tag5:12)"  + "\t" + "(tag6:11)" + "\t" + "(tag7:5)" + "\t" + "(tag8:4)" + "\t" + "(tag9:3)" + "\t" + "(tag10:2)"));
		}catch (Exception e){
			e.printStackTrace();
		}
		
	}
}
