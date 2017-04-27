package task3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import task3.TextTextPair;

// input (PlaceName, tag) 1
// output (PlaceName, total) tag

public class TagFrequentCombiner extends Reducer<TextTextPair, Text, TextTextPair, Text>{
    public void reduce(TextTextPair key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
        int result = 0;
        //String place_name = key.getKey().toString();
        //String tag = key.getOrder().toString();
        for (Text value : values){
            result += Integer.parseInt(value.toString());
        }
        context.write(key, new Text(Integer.toString(result)));
        
    }
}