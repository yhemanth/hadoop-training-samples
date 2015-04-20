package com.dsinpractice.samples.hadoop.mapred.customformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MyKey implements WritableComparable {

	private Text imei, deviceModel;

	public MyKey() {
		setImei(new Text());
		setDeviceModel(new Text());
	}

	public MyKey(Text _imei, Text _deviceModel) {
		setImei(_imei);
		setDeviceModel(_deviceModel);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		getImei().readFields(in);
		getDeviceModel().readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		getImei().write(out);
		getDeviceModel().write(out);
	}

	@Override
	public int compareTo(Object obj) {
		MyKey argKey = (MyKey) obj;
		int result = this.getImei().compareTo(argKey.getImei());
		if(result != 0){
			return result;
		}
		return this.getDeviceModel().compareTo(argKey.getDeviceModel());
	}

	public Text getImei() {
		return imei;
	}

	public void setImei(Text imei) {
		this.imei = imei;
	}

	public Text getDeviceModel() {
		return deviceModel;
	}

	public void setDeviceModel(Text deviceModel) {
		this.deviceModel = deviceModel;
	}

}
