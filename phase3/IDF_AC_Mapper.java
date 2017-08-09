import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class IDF_AC_Mapper extends Mapper<LongWritable, Text, Text, Text>{
	private int indexCount = 0;
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		//split the value
		String[] values = value.toString().split(" ");
		
		// clean up the values
		for(int i = 0; i < values.length; i++)
		{
			values[i].trim();
		}
		
		//send key/value pair to reducer
		String out = values[1] + " " + indexCount + " " + values[2];
		indexCount++;
		context.write(new Text(values[0]), new Text(out));
	}

}
