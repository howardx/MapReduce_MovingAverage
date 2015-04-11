package SecondarySort_with_MovingAverage_MR;
/*
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class IndividualStockMovingAverage_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
  public void reduce(Text text, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException
  {
    context.write(text, new IntWritable(sum));
  }
}
*/