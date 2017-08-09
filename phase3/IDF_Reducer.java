import java.io.IOException;
import java.lang.Math;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IDF_Reducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		double AC = 0;
		double AWC = 0;
		String index = null;
		double IDF;
		
		for(Text value: values)
		{
			String[] valArray = value.toString().split(" ");
			for(int i = 0; i < valArray.length; i++)
			{
				valArray[i].trim();
			}
			
			if(valArray.length == 3)
			{
				AC = Double.parseDouble(valArray[0]);
				index = valArray[1];
			}
			else if(valArray.length == 1)
			{
				AWC = Double.parseDouble(valArray[0]);
			}
			else
			{
				context.write(key, new Text("ERROR"));
			}
		}
		
		IDF = Math.log10((AC/AWC));
		String out = " " + index + " " + IDF;
		context.write(key, new Text(out));
		
	}

}
