package task1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// input PlaceName \t 1
// output PlaceName \t Count

public class PlaceFilterReducer extends Reducer<Text, Text, Text, Text>{
    
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            Text result = new Text();
            int counter = 0;
            for(Text text : values){
                counter += Integer.parseInt(text.toString());
            }
            result.set(Integer.toString(counter));
            context.write(key, result);
    }
    
}