import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IDF_TF_Reducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		HashMap<String, String> author_TF = new HashMap<String, String>();
		String index = null;
		String IDF = null;
		String maxIndex = null;
		boolean test = true;
		boolean test1 = false;
		
		for(Text value: values)
		{
			String[] valArray = value.toString().split(" ");
			
			for(int i = 0; i < valArray.length; i++)
			{
				valArray[i].trim();
			}
			
			if(valArray.length == 3)// IDF
			{
				IDF = valArray[1];
				maxIndex = valArray[2];
				index = valArray[0];
				//if(IDF.equals(null) || maxIndex.equals(null) || index.equals(null))
				//{
				//	context.write(key, new Text("NULL"));
			//	}
				//else
				//{
				//	context.write(key, new Text(IDF + " " + maxIndex + " " + index));
				//}
				//test = false;
				//test1 = true;
			}
			else if(valArray.length == 2)// TF
			{
				author_TF.put(valArray[0], valArray[1]);
				//context.write(key, new Text("---" + valArray[0] + "---" + valArray[1]));
				//test = false;
			}
			else
			{
				context.write(key, new Text("ERROR   " + valArray.length));
				test = false;
			}
		}
		
		Iterator iterator = author_TF.entrySet().iterator();
		while(iterator.hasNext() && test)
		{
			Map.Entry next = (Map.Entry)iterator.next();
			Double TF = Double.parseDouble((String)next.getValue());
			Double IDF_ = Double.parseDouble(IDF);
			Double IDF_TF = (IDF_ * TF);
			
			String out = " " + index + " " + (String)next.getKey() + " " + IDF_TF + " " + maxIndex;
			context.write(key, new Text(out));
		}
	}

}
