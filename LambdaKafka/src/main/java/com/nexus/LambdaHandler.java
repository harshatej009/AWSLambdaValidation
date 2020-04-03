package com.nexus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class LambdaHandler implements RequestHandler<S3Event, String> {

	final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
			.withRegion(Regions.US_EAST_1)
			.build();
	private String sourceBucket="carefirst-nexus-project";
	private String targetBucket="carefirst-nexus-project-destination";
	private String filename="lambdaMapping.csv";
	
	public String handleRequest(S3Event input, Context context) {
		
		S3EventNotificationRecord record = input.getRecords().get(0);
		String srcBucket = record.getS3().getBucket().getName();
		context.getLogger().log("Event is generated with : " + srcBucket);
		
		S3Object s3object = s3.getObject(sourceBucket, filename);
		S3ObjectInputStream file = s3object.getObjectContent();
		int validation = validateCsv(file);
		
		if (validation == 0) {
			context.getLogger().log("VALID FILE " + srcBucket);
			s3.copyObject(sourceBucket, filename, targetBucket, filename);
		} else {
			context.getLogger().log("IN VALID FILE " + srcBucket);
		}
		return "JOB FINISHED";
	}

	public int validateCsv(S3ObjectInputStream file) {
		String line = "";
		BufferedReader br = null;
		int count = 0;
		
		try {
			br = new BufferedReader(new InputStreamReader(file));
			while ((line = br.readLine()) != null) {
				String[] attribute = line.split(",");
				for (int i = 0; i < attribute.length; i++) {
					if (attribute[i].length() < 2) {
						count = count + 1;
					}
				}
			}
		} catch (IOException e) {
			System.err.println("IN Valid " + e.getMessage());
		} finally {
			try {
				br.close();
				file.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return count;
	}
}
