import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AAVMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] values = value.toString().split(" ");
		
		for(int i = 0; i < values.length; i++)
		{
			values[i] = values[i].trim();
		}
		
		String out = values[0] + " " + values[1] + " " + values[3] + " " + values[4];
		context.write(new Text(values[2]), new Text(out));
	}
}
