import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MaxIndexReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		HashMap<String, String> words = new HashMap<String, String>();
		
		for(Text value: values)
		{
			String[] valArray = value.toString().split(" ");
			
			for(int i = 0; i < valArray.length; i++)
			{
				valArray[i].trim();
			}
			
			String temp = valArray[1] + ":" + valArray[2];
			words.put(valArray[0], temp);
		}
		
		Iterator iterator1 = words.entrySet().iterator();
		Integer max = 0;
		while(iterator1.hasNext())
		{
			Map.Entry next = (Map.Entry)iterator1.next();
			String[] arr = ((String)next.getValue()).split(":");
			if(Integer.parseInt(arr[0]) > max)
			{
				max = Integer.parseInt(arr[0]);
			}
		}
		
		Iterator iterator2 = words.entrySet().iterator();
		while(iterator2.hasNext())
		{
			Map.Entry next = (Map.Entry)iterator2.next();
			String[] arr = ((String)next.getValue()).split(":");
			String out = " " + arr[0] + " " + arr[1] + " " + max;
			context.write(new Text((String)next.getKey()), new Text(out));
		}
	}

}
