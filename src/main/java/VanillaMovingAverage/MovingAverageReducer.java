package VanillaMovingAverage;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovingAverageReducer extends
  Reducer<TimeSeriesKey, DoubleWritable, Text, DoubleWritable>
{
  private final int windowSize = 5;
  Queue<Double> window = new LinkedList<Double>();
  
  public void reduce(TimeSeriesKey key,
    Iterable<DoubleWritable> values, Context context) throws
    IOException, InterruptedException
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
        return;
      }
      else // window.size() == windowSize
      {
        context.write(key.getDate(), new DoubleWritable( calculateAverage() ));
        window.poll();
        return;
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
