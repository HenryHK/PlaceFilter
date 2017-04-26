package task3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PlaceNumTagReducer extends Reducer<Text, Text, Text, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            for(Text text: values){
                String[] v = text.toString().split("\t");
                context.write(new Text(v[0]), new Text(key.toString()+"\t"+v[1]));
            }
    }

}