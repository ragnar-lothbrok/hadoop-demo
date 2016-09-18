package com.hadoop.intellipaat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountWordPerFile implements Writable {

	private String fileName;
	private Integer count;

	public CountWordPerFile() {

	}

	public CountWordPerFile(String fileName, Integer count) {
		super();
		this.fileName = fileName;
		this.count = count;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(fileName);
		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.fileName = in.readUTF();
		this.count = in.readInt();

	}

	public String getFileName() {
		return fileName;
	}

	public Integer getCount() {
		return count;
	}

	@Override
	public String toString() {
		return "CountWordPerFile [fileName=" + fileName + ", count=" + count + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CountWordPerFile other = (CountWordPerFile) obj;
		if (fileName == null) {
			if (other.fileName != null)
				return false;
		} else if (!fileName.equals(other.fileName))
			return false;
		return true;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

}
