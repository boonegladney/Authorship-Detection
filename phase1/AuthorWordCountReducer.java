import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AuthorWordCountReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		ArrayList<String> authors = new ArrayList<String>();
		
		// iterate through values
		for(Text value: values)
		{
			boolean found = false;
			//check if the author was already added to authors
			for(int i = 0; i < authors.size(); i++)
			{
				if(authors.get(i).equals(value.toString()))
					found = true;
			}
			
			//if the author was not added to authors, add it.
			if(!found)
			{
				authors.add(value.toString());
			}
		}
		
		String out = (" " + authors.size());
		context.write(key, new Text(out));
	}
}
