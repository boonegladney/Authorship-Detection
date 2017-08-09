import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AAVReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		boolean b = false;
		String[] words = null;
		
		for(Text value: values)
		{
			String[] valArray = value.toString().split(" ");
			for(int i = 0; i < valArray.length; i++)
			{
				valArray[i] = valArray[i].trim();
			}
			
			if(b == false)
			{
				words = new String[Integer.parseInt(valArray[3]) + 1];
				for(int i = 0; i < words.length; i++)
				{
					words[i] = "0.0";
				}
				b = true;
			}
			
			words[Integer.parseInt(valArray[1])] = valArray[2];
		}
		
		StringBuilder out = new StringBuilder();
		
		for(int i = 0; i < words.length; i++)
		{
			out.append((" " + words[i]));
		}
		
		context.write(key, new Text(out.toString()));
	}

}
