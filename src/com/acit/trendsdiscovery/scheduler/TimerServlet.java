package com.acit.trendsdiscovery.scheduler;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Timer;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


 
@WebServlet("/cron/trendsDiscoveryTask")
public class TimerServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	TaskScheduler schedulerJobs = new TaskScheduler();
	int array = -1;

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

		add(request, response);
	}

	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
		doGet(request, response);
	}

	void add(HttpServletRequest request, HttpServletResponse response) throws IOException {
		{
			System.out.println("cron job method called");

			String jobName = "Trends discovery job";
			//int interval = 60;
			//int interval=1440;
			//Monthly
			int interval=43200;
			boolean state = true;

			schedulerJobs = new TaskScheduler();
			schedulerJobs.setCronName(jobName);
			schedulerJobs.setInterval(interval);
			schedulerJobs.setEnableState(state);

			System.out.println("State Interval::" + state + interval);
			// running timer task as daemon thread
			Timer timer = new Timer(true);
			timer.scheduleAtFixedRate(schedulerJobs, 0, interval * 60 * 1000);
			// return
			PrintWriter out = response.getWriter();
			out.println(jobName + " successful");
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			return;
		}
	}
}
