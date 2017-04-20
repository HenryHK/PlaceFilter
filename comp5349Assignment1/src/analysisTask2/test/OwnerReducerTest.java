package analysisTask2.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import analysisTask2.OwnerReducer;

//A unit test for OwnerReducer
//It contains a single test method to test the valid final result using the output of mappers
//@author Zhao Liu

public class OwnerReducerTest 
{
	@Test
	public void processValidRecord() throws IOException
	{
		
		OwnerReducer reducer = new OwnerReducer();
		
		Text key = new Text("Country1");
		Text country = new Text();
		ArrayList<Text> values = new ArrayList<Text>();
		values.add(new Text("locality1" + "\t" + "owner1"));
		values.add(new Text("locality1" + "\t" + "owner2"));
		values.add(new Text("locality1" + "\t" + "owner3"));
		values.add(new Text("locality1" + "\t" + "owner4"));
		values.add(new Text("locality1" + "\t" + "owner5"));
		values.add(new Text("locality2" + "\t" + "owner1"));
		values.add(new Text("locality2" + "\t" + "owner2"));
		values.add(new Text("locality2" + "\t" + "owner3"));
		values.add(new Text("locality2" + "\t" + "owner4"));
		values.add(new Text("locality3" + "\t" + "owner1"));
		values.add(new Text("locality3" + "\t" + "owner2"));
		values.add(new Text("locality3" + "\t" + "owner3"));
		values.add(new Text("locality4" + "\t" + "owner1"));
		values.add(new Text("locality4" + "\t" + "owner2"));
		values.add(new Text("locality5" + "\t" + "owner1"));
		
		Iterable<Text> iterableValue = values;
		Reducer.Context mock = mock(Reducer.Context.class);
		Reducer<Text, Text, Text, Text>.Context context = mock;
		try{
		reducer.reduce(key, iterableValue, context);
		country.set(key);
		verify(context).write(country, new Text("\t" + "(locality1:5)" + "\t" + "(locality2:4)" + "\t" + "(locality3:3)"  + "\t" + "(locality4:2)" + "\t" + "(locality5:1)"));
		}catch (Exception e){
			e.printStackTrace();
		}
		
	}
}
