import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TFReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		// count the frequency of each word.
		for(Text value: values)
		{
			if(map.containsKey(value.toString()))
			{
				Integer oldVal = map.get(value.toString());
				map.put(value.toString(), (oldVal + 1));
			}
			else
				map.put(value.toString(), 1);
		}
		
		// find the max frequency
		Iterator iterator1 = map.entrySet().iterator();
		Integer max = 0;
		while(iterator1.hasNext())
		{
			Map.Entry keyVal = (Map.Entry)iterator1.next();
			Integer temp = (Integer)keyVal.getValue();
			if(temp > max)
				max = temp;
		}
		
		// calculate the TF for each word and write the key/val to the output.
		Iterator iterator2 = map.entrySet().iterator();
		while(iterator2.hasNext())
		{
			Map.Entry keyVal = (Map.Entry)iterator2.next();
			Integer frequency = (Integer)keyVal.getValue();
			double TF = (0.5 + (0.5 * ((double)frequency.intValue()/(double)max.intValue())));
			String out = (" " + (String)keyVal.getKey() + " " + TF);
			context.write(key, new Text(out));
			iterator2.remove();
		}
	}

}
