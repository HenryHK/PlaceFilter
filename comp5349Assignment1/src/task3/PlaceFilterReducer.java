package task3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// input PlaceName \t 1 \t tags
// output PlaceName \t Count \t tags

public class PlaceFilterReducer extends Reducer<Text, Text, Text, Text>{
    
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            Text result = new Text();
            StringBuffer tags = new StringBuffer();
            int counter = 0;
            for(Text text : values){
                String[] v = text.toString().split("\t");
                counter += Integer.parseInt(v[0].toString());
                if(v.length > 1)
                    tags.append(v[1]+" ");
            }
            result.set(Integer.toString(counter) + "\t" + tags.toString());
            context.write(key, result);
    }
    
}