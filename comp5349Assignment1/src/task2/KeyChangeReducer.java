package task2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyChangeReducer extends Reducer<Text, Text, Text, Text>{

    //HashMap<Text, Text> map = new HashMap<Text, Text>();
    private int counter = 0;

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            //int counter=0;
            if(counter++ < 50){
                for(Text text: values){
                    context.write(text, key);
                }
            }
    }

}