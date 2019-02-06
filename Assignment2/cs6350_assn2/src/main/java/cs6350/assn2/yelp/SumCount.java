package cs6350.assn2.yelp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class SumCount implements WritableComparable<SumCount> {

	DoubleWritable sum;
	IntWritable count;

	public SumCount() {
		set(new DoubleWritable(0), new IntWritable(0));
	}

	public SumCount(Double sum, Integer count) {
		set(new DoubleWritable(sum), new IntWritable(count));
	}

	public void set(DoubleWritable sum, IntWritable count) {
		this.sum = sum;
		this.count = count;
	}

	public DoubleWritable getSum() {
		return sum;
	}

	public IntWritable getCount() {
		return count;
	}

	public void addSumCount(SumCount sumCount) {
		set(new DoubleWritable(this.sum.get() + sumCount.getSum().get()),
				new IntWritable(this.count.get() + sumCount.getCount().get()));
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {

		sum.write(dataOutput);
		count.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {

		sum.readFields(dataInput);
		count.readFields(dataInput);
	}

	@Override
	public int compareTo(SumCount sumCount) {
		//compare mean values between two sum count
		double mean1 = this.getSum().get() / this.getCount().get();
		double mean2 = sumCount.getSum().get() / sumCount.getCount().get();
		if(mean1 > mean2)
			return 1 ;
		if (mean1 < mean2)
			return -1;
		return 0;		
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		SumCount sumCount = (SumCount) o;

		return count.equals(sumCount.count) && sum.equals(sumCount.sum);
	}

	@Override
	public int hashCode() {
		int result = sum.hashCode();
		result = 31 * result + count.hashCode();
		return result;
	}
}