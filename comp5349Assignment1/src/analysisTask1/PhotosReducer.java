package analysisTask1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//This reducer is used to output numberOfPhotos in top 50
//Input format: numberOfPhotos \t place_name \t (tags:freq)+
//Output format: place_name \t numberOfPhotos \t (tags:freq)+
//@author Zhao Liu

public class PhotosReducer extends Reducer<Text, Text, Text, Text> 
{
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private int top50 = 0;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		if (top50 < 50)
		{
			String photoFrequency = key.toString();
			String tagsList = "";
			String place_name = "";
			
			//Read each value
			for (Text text: values)
			{
				String placeAndTags = text.toString();

				tagsList = placeAndTags.substring(placeAndTags.indexOf("\t") + 1);
				place_name = placeAndTags.substring(0, placeAndTags.indexOf("\t"));

			}
			keyOut.set(place_name.trim());
//			value.set("\t" + photoFrequency.trim() + "\t" + tagsList.trim());
			valueOut.set(photoFrequency.trim() + "\t" + tagsList.trim());
			context.write(keyOut, valueOut);
			top50++;
			
		}
			

	}

}

