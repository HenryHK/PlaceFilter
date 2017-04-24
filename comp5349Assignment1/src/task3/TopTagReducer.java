package task3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import task3.TextTextPair;

// input (PlaceName, tag) 1
// output (PlaceName, total) tag

public class TopTagReducer extends Reducer<TextIntPair, Text, Text, Text>{
    public void reduce(TextIntPair key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
        int result = 0;
        String place_name = key.getKey().toString();
        int total = key.getOrder().get();
        for (Text value : values){
            String tag = value.toString();
            context.write(new Text(place_name), new Text(Integer.toString(total) + "\t" +tag));
        }
        
        
    }
}