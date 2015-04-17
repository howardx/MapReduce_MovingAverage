package singleTimeSeriesSMA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TimeSeriesKey implements WritableComparable<TimeSeriesKey>
{
  private IntWritable year = new IntWritable();
  private IntWritable month = new IntWritable();
  private IntWritable day = new IntWritable();
  
  private Text date = new Text();

  /*
   * either create a default constructor (no input args)
   * or have no constructors at all
   */

  public void parseDate(String d)
  {
	date.set(d);
    String [] dateList = d.split("-");
    
    year.set(Integer.valueOf(dateList[0]));
    month.set(Integer.valueOf(dateList[1]));
    day.set(Integer.valueOf(dateList[2]));
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
