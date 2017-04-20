package analysisTask1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PlaceFilterReducer extends Reducer<Text, Text, Text, Text>{
    Text result = new Text();
    int counter = 0;
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            for(Text text : values){
                counter += Integer.parseInt(text.toString());
            }
            result.set(Integer.toString(counter));
            context.write(key, result);
    }
    
}