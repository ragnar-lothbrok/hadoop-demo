package com.hadoop.intellipaat;

import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class JobRunner implements Runnable{

	private JobControl control;

	public JobRunner(JobControl _control) {
		this.control = _control;
	}

	@Override
	public void run() {
		this.control.run();
	}

}
