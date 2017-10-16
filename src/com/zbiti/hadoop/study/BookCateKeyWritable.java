package com.zbiti.hadoop.study;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

// define a new WritableComparable subClass
public class BookCateKeyWritable implements WritableComparable<BookCateKeyWritable> {
	private Text cateId;
	private IntWritable count;

	public BookCateKeyWritable() {
		setBookCateKeyWritable(new Text(), new IntWritable());
	}

	public void setBookCateKeyWritable(Text cateId, IntWritable count) {
		this.cateId = cateId;
		this.count = count;
	}

	public void write(DataOutput out) throws IOException {
		cateId.write(out);
		count.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		cateId.readFields(in);
		count.readFields(in);
	}

	public int compareTo(BookCateKeyWritable o) {
		int firstCompare = -(this.count.compareTo(o.count));
		if (firstCompare == 0) {
			return -this.cateId.compareTo(o.cateId);
		}
		return firstCompare;
	}

	public Text getCateId() {
		return cateId;
	}

	public void setCateId(Text cateId) {
		this.cateId = cateId;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

}