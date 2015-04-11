package SecondarySort_with_MovingAverage_MR;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StockRecord implements WritableComparable<StockRecord>
{
  @Override
  public void readFields(DataInput arg0) throws IOException {
  	
  }
  
  @Override
  public void write(DataOutput arg0) throws IOException {
  	
  }
  
  @Override
  public int compareTo(StockRecord o) {
  	return 0;
  }

}
