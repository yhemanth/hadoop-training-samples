package com.dsinpractice.samples.hadoop.mapred.customformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MyValue implements WritableComparable {

	private Text status, country, basePhone, advPhone;

	public MyValue() {
		this.setStatus(new Text());
		this.setCountry(new Text());
		this.setBasePhone(new Text());
		this.setAdvPhone(new Text());
	}

	public MyValue(Text _status, Text _country, Text _basePhone, Text _advPhone) {
		this.setStatus(_status);
		this.setCountry(_country);
		this.setBasePhone(_basePhone);
		this.setAdvPhone(_advPhone);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.getStatus().readFields(in);
		this.getCountry().readFields(in);
		this.getBasePhone().readFields(in);
		this.getAdvPhone().readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.getStatus().write(out);
		this.getCountry().write(out);
		this.getBasePhone().write(out);
		this.getAdvPhone().write(out);

	}

	@Override
	public int compareTo(Object obj) {
		MyValue argVal = (MyValue) obj;
		int compare = this.getStatus().compareTo(argVal.getStatus());
		if (compare != 0) {
			return compare;
		}
		compare = this.getCountry().compareTo(argVal.getCountry());
		if (compare != 0) {
			return compare;
		}
		compare = this.getBasePhone().compareTo(argVal.getBasePhone());
		if (compare != 0) {
			return compare;
		}
		return this.getAdvPhone().compareTo(argVal.getAdvPhone());
	}

	public Text getStatus() {
		return status;
	}

	public void setStatus(Text status) {
		this.status = status;
	}

	public Text getCountry() {
		return country;
	}

	public void setCountry(Text country) {
		this.country = country;
	}

	public Text getBasePhone() {
		return basePhone;
	}

	public void setBasePhone(Text basePhone) {
		this.basePhone = basePhone;
	}

	public Text getAdvPhone() {
		return advPhone;
	}

	public void setAdvPhone(Text advPhone) {
		this.advPhone = advPhone;
	}

}
