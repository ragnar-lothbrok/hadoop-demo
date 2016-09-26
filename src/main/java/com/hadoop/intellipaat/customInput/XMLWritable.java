package com.hadoop.intellipaat.customInput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class XMLWritable implements WritableComparable {

	private String id;
	private String first_name;
	private String last_name;
	private String email;
	private String gender;
	private String ip_address;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.id);
		out.writeUTF(this.first_name);
		out.writeUTF(this.last_name);
		out.writeUTF(this.email);
		out.writeUTF(this.gender);
		out.writeUTF(this.ip_address);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readUTF();
		this.first_name = in.readUTF();
		this.last_name = in.readUTF();
		this.email = in.readUTF();
		this.gender = in.readUTF();
		this.ip_address = in.readUTF();
	}

	@Override
	public int compareTo(Object o) {
		return this.id.compareTo(((XMLWritable) o).id);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getFirst_name() {
		return first_name;
	}

	public void setFirst_name(String first_name) {
		this.first_name = first_name;
	}

	public String getLast_name() {
		return last_name;
	}

	public void setLast_name(String last_name) {
		this.last_name = last_name;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getIp_address() {
		return ip_address;
	}

	public void setIp_address(String ip_address) {
		this.ip_address = ip_address;
	}

	@Override
	public String toString() {
		return "XMLWritable [id=" + id + ", first_name=" + first_name + ", last_name=" + last_name + ", email=" + email + ", gender=" + gender
				+ ", ip_address=" + ip_address + "]";
	}
	
	public String toCSV() {
		return "" + id + ", " + first_name + ", " + last_name + ", " + email + ", " + gender
				+ ", " + ip_address + "";
	}

}
