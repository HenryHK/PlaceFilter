package analysisTask1.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import analysisTask1.PhotosReducer;

//A unit test for PhotosReducer
//It contains a single test method to test the valid result of final result using the output of mappers
//@author Zhao Liu

public class PhotosReducerTest 
{
	@Test
	public void processValidRecord() throws IOException
	{
		
		PhotosReducer reducer = new PhotosReducer();
		
		Text key = new Text("3");
		Text placeName = new Text();
		ArrayList<Text> values = new ArrayList<Text>();
		values.add(new Text("'s-Heer-Abtskerke, Zeeland, Netherlands	(24:3)	(2door:3)	(42:3)	(52:3)	(8:3)	(ag:3)	(audi:3)	(auto:3)	(automotive:3)	(black:3)"));
		Iterable<Text> iterableValue = values;
		Reducer.Context mock = mock(Reducer.Context.class);
		Reducer<Text, Text, Text, Text>.Context context = mock;
		try{
		reducer.reduce(key, iterableValue, context);
		placeName.set("'s-Heer-Abtskerke, Zeeland, Netherlands");
		verify(context).write(placeName, new Text("3" + "\t" + "(24:3)	(2door:3)	(42:3)	(52:3)	(8:3)	(ag:3)	(audi:3)	(auto:3)	(automotive:3)	(black:3)"));
		}catch (Exception e){
			e.printStackTrace();
		}		
	}
}
