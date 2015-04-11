package VanillaMovingAverage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Mapper<LongWritable, Text, TimeSeriesKey, DoubleWritable>
 * Mapper<lineOffset from recordReader, line, mapper output key, mapper output value>
 * 
 * line offset and line (recordReader output) may change as recordReader can be extended
 */
public class MovingAverageMapper extends
  Mapper<LongWritable, Text, TimeSeriesKey, DoubleWritable>
{
  public void map(IntWritable lineOffset, Text recordLine, 
    Context context) throws IOException, InterruptedException
  {
	// csv file
    String [] line = recordLine.toString().split(",");
    // the last column is price
    DoubleWritable price = new DoubleWritable(Double.valueOf(line[line.length-1]));
    String date = line[0];
    
    TimeSeriesKey key = new TimeSeriesKey(date);
    context.write(key, price);
  }
}