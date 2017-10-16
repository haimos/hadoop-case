package com.zbiti.hadoop.study;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

// define a new WritableComparable subClass
public class OrderEntityWritable implements WritableComparable<OrderEntityWritable> {
	private Text sellunitCode;
	private IntWritable count;

	public OrderEntityWritable() {
		setOrderEntityWritable(new Text(), new IntWritable());
	}

	public void setOrderEntityWritable(Text sellunitCode, IntWritable count) {
		this.sellunitCode = sellunitCode;
		this.count = count;
	}

	public void write(DataOutput out) throws IOException {
		sellunitCode.write(out);
		count.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		sellunitCode.readFields(in);
		count.readFields(in);
	}

	public int compareTo(OrderEntityWritable o) {
		int firstCompare = -(this.count.compareTo(o.count));
		if (firstCompare == 0) {
			return this.sellunitCode.compareTo(o.sellunitCode);
		}
		return firstCompare;
	}

	public Text getSellunitCode() {
		return sellunitCode;
	}

	public void setSellunitCode(Text sellunitCode) {
		this.sellunitCode = sellunitCode;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

}