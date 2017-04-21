package analysisTask1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyChangeReducer extends Reducer<Text, Text, Text, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            for(Text text: values){
                context.write(text, key);
            }
    }

}