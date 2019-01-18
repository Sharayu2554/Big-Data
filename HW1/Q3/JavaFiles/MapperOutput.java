package Q3MeanVariance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class MapperOutput implements Writable{
	
	private DoubleWritable sumOfNo = new DoubleWritable();
	private IntWritable countOfNo = new IntWritable();
	private DoubleWritable sumSquareOfNo = new DoubleWritable();
	
	
	public MapperOutput() {
		super();
		set(new DoubleWritable(0), new IntWritable(0), new DoubleWritable(0));
	}
	
	public MapperOutput(DoubleWritable sumOfNo, IntWritable countOfNo, DoubleWritable sumSquareOfNo) {
		super();
		this.sumOfNo = sumOfNo;
		this.countOfNo = countOfNo;
		this.sumSquareOfNo = sumSquareOfNo;
	}

	public void set(DoubleWritable sumOfNo, IntWritable countOfNo, DoubleWritable sumSquareOfNo) {
        this.sumOfNo = sumOfNo;
        this.countOfNo = countOfNo;
		this.sumSquareOfNo = sumSquareOfNo;
    }
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		sumOfNo.readFields(in);
		countOfNo.readFields(in);
		sumSquareOfNo.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		sumOfNo.write(out);
		countOfNo.write(out);
		sumSquareOfNo.write(out);
	}

	public Double getSumOfNo() {
		return sumOfNo.get();
	}

	public void setSumOfNo(DoubleWritable sumOfNo) {
		this.sumOfNo = sumOfNo;
	}

	public int getCountOfNo() {
		return countOfNo.get();
	}

	public void setCountOfNo(IntWritable countOfNo) {
		this.countOfNo = countOfNo;
	}

	public Double getSumSquareOfNo() {
		return sumSquareOfNo.get();
	}

	public void setSumSquareOfNo(DoubleWritable sumSquareOfNo) {
		this.sumSquareOfNo = sumSquareOfNo;
	}

	@Override
	public String toString() {
		return "MapperOutput [sumOfNo=" + sumOfNo + ", countOfNo=" + countOfNo + ", sumSquareOfNo=" + sumSquareOfNo
				+ "]";
	}

	

}
