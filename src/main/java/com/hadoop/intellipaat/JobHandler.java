package com.hadoop.intellipaat;

import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class JobHandler {

	public static void handleRun(JobControl control) {
		JobRunner runner = new JobRunner(control);
		Thread t = new Thread(runner);
		t.start();

		while (!control.allFinished()) {
			System.out.println("Still running...");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
