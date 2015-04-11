package VanillaMovingAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TimeSeriesKey implements 
  WritableComparable<TimeSeriesKey>
{
  private IntWritable year;
  private IntWritable month;
  private IntWritable day;
  
  private Text date;
  
  // index act as month
  private final int [] dayInMonth =
    {31,28,31,30,31,30,31,31,30,31,30,31};
  
  public TimeSeriesKey(String date)
  {
	this.date = new Text(date);
    String [] dateList = date.split("-");
    year = new IntWritable(Integer.valueOf(dateList[0]));
    month = new IntWritable(Integer.valueOf(dateList[1]));
    day = new IntWritable(Integer.valueOf(dateList[2]));
  }
 
  @Override
  public int compareTo(TimeSeriesKey compare)
  {
    if (year.compareTo(compare.getYear()) == 0)
    {
      if (month.compareTo(compare.getMonth()) == 0)
      {
        return day.compareTo(compare.getDay());
      }
      else
      {
        return month.compareTo(compare.getMonth());
      }
    }
    else
    {
      return year.compareTo(compare.getYear());
    }
  }
  
  public IntWritable getYear() {
	return year;
  }
  
  public void setYear(IntWritable year) {
  	this.year = year;
  }
  
  public IntWritable getMonth() {
  	return month;
  }
  
  public void setMonth(IntWritable month) {
  	this.month = month;
  }
  
  public IntWritable getDay() {
  	return day;
  }
  
  public void setDay(IntWritable day) {
  	this.day = day;
  }
  
  @Override
  public void readFields(DataInput arg0) throws IOException {}
  
  @Override
  public void write(DataOutput arg0) throws IOException {}

  public Text getDate() {
  	return date;
  }
  
  public void setDate(Text date) {
  	this.date = date;
  }
}
