package singleTimeSeriesSMA;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovingAverageReducer extends
  Reducer<TimeSeriesKey, DoubleWritable, Text, DoubleWritable>
{
  // output should have (inputRowNumber - windowSize) number of rows
  private final int windowSize = 20; 
  private Queue<Double> window = new LinkedList<Double>();
  
  /*
   * Map Reduce reuses objects (key/value)
   */
  public Text outputKey = new Text();
  
  public void reduce(TimeSeriesKey key, Iterable<DoubleWritable> values,
    Context context) throws IOException, InterruptedException
  {
    for (DoubleWritable value : values)
    {
      /*
       * if window is not full, need to return and wait till the 
       * next (key, value) pair from mapper in order to fill it
       */
      if (window.size() < windowSize)
      {
        window.add( Double.valueOf( value.toString() ) );
      }
      else // window.size() == windowSize
      {
    	outputKey = key.getDate();
    	
        context.write(outputKey, new DoubleWritable( calculateAverage() ));
        window.poll();

        /* after poping from head of the queue, new value from current 
         * iteration should be added 
         */
        window.add( Double.valueOf( value.toString() ) );
      }
    }
  }
  
  private Double calculateAverage()
  {
    Double sum = 0.0;
    for (Double d : window)
    {
      sum = sum + d;
    }
    return sum/windowSize;
  }
}
