package com.apache.beam.examples.jsonetl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


class LoadFileInMySQL extends Thread {

	public String filePath;

	public LoadFileInMySQL(String filePath) {
    this.filePath = filePath;	    
	}

	@Override
	public void run() {			

		Connection connection = null;
        try {
        	connection = DriverManager.getConnection("jdbc:mysql://auroracluster-instance-1.crgger8roage.ap-south-1.rds.amazonaws.com:3308/auroraTestDB?allowLoadLocalInfile=true", "auroraAdmin","auroraPass");
            //connection = DriverManager.getConnection("jdbc:mysql://rds-msql.crgger8roage.ap-south-1.rds.amazonaws.com:6030/rdstest?allowLoadLocalInfile=true&rewriteBatchedStatements=true", "dbadmin","adminpass");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        Statement statement = null;
        try {
        	statement = connection.createStatement();
        	statement.execute("load data FROM S3 \"" + filePath.replaceAll("\\\\", "/") + "\" into table auroraTestDB.sample LINES TERMINATED BY '\\n'");
        	//statement.execute("load data CONCURRENT local infile \"" + filePath.replaceAll("\\\\", "/") + "\" into table rdstest.sample LINES TERMINATED BY '\\n'");
              
	        } catch(SQLException e) {
	        	e.printStackTrace();
	        } 
	        finally {
	            if (statement != null) {
	                try {
	                    statement.close();
	                } catch (SQLException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	        if (connection != null) {
	            try {
	            	
	                connection.close();
	            } catch (SQLException e) {
	                e.printStackTrace();
	            }
	        }		
		}
	}

public class JsonETLLambda implements RequestHandler<Map<String,String>, String> {
	
//	private static ExecutorService threadPool = Executors.newFixedThreadPool(50);
	
	public interface JsonTrialOptions extends PipelineOptions, S3Options {

		String getBucketInputUrl();

		void setBucketInputUrl(String value);

//		@Required
//		String getOutput();
//
//		void setOutput(String value);

	    @Description("Import location in the format gs://<BUCKET_NAME> or s3://<BUCKET_NAME>")
	    @Default.String("gs://my-bucket")
	    String getBucketOutputUrl();

	    void setBucketOutputUrl(String bucketUrl);
	    
	    @Description("AWS S3 Key ID")
	    @Default.String("KEY")
	    String getAwsAccessKey();

	    void setAwsAccessKey(String awsAccessKey);

	    @Description("AWS S3 Secret Key")
	    @Default.String("SECRET KEY")
	    String getAwsSecretKey();

	    void setAwsSecretKey(String awsSecretKey);

	}
	
	@SuppressWarnings("serial")
	static void runJsonTransform(JsonTrialOptions options) throws InterruptedException {

		final HashMap<String, Object> hashmap = new HashMap<String, Object>();

		final ObjectMapper jacksonObjMapper = new ObjectMapper();

		Pipeline p = Pipeline.create(options);

		long beginTime = System.currentTimeMillis();

		PCollection<String> lines = p.apply(TextIO.read().from(options.getBucketInputUrl()+ "/*.json"));
		
		PCollection<String> data = lines.apply(MapElements.via(new SimpleFunction<String, String>(){
			       		
			       		@Override
			       		public String apply(String input) {
			       		 
			                try {
			                	int sum=0;
			                    JsonNode jsonNode = jacksonObjMapper.readTree(input);
			                    
			                    String avgString = jsonNode.get("phone").textValue().replaceAll("[+()^\\s]", "").trim();
			                    
			                    for(int i=0; i<avgString.length();i++) {
			                    	sum=sum + avgString.charAt(i);
			                    }
			                    
			                    hashmap.put("fname", jsonNode.get("fname").textValue().trim());
			                    hashmap.put("age", jsonNode.get("age").intValue());
			                    hashmap.put("empId", jsonNode.get("fname").textValue().trim().substring(0,2).toUpperCase() + jsonNode.get("age").intValue());
			                    hashmap.put("avg", sum/avgString.length());
			                    
			                    String json = jacksonObjMapper.writeValueAsString(hashmap);
			          		  
			                    return json;	                    
			                   
			                } catch (JsonProcessingException e) {
			                    e.printStackTrace();
			                    return null;
			                }
			                catch (@SuppressWarnings("hiding") IOException e) {
			                    e.printStackTrace();
			                    return null;
			                }
			       		}                
			       	}));
			    
				data.apply(TextIO.write().to(options.getBucketOutputUrl()));

		p.run().waitUntilFinish();
		
		long endTime = System.currentTimeMillis();
		System.out.println((endTime - beginTime) / 1000 + " --------*********Extract Transform total seconds");

		long beginTime1 = System.currentTimeMillis();
		
//		Connection connection = null;
//        try {
//        	connection = DriverManager.getConnection("jdbc:mysql://auroracluster-instance-1.crgger8roage.ap-south-1.rds.amazonaws.com:3308/auroraTestDB?allowLoadLocalInfile=true", "auroraAdmin","auroraPass");
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        try {
//            System.out.println("Established connection to " + connection.getMetaData().getURL());
//        } catch (SQLException e1) {
//            e1.printStackTrace();
//        }
//
//		String path = null;
//
//		if (options.getBucketOutputUrl().lastIndexOf("\\") == -1) {
//			path = options.getBucketOutputUrl();
//		} else {
//			path = options.getBucketOutputUrl().substring(0, options.getBucketOutputUrl().lastIndexOf("\\"));
//		}

//		AmazonS3 s3client = AmazonS3ClientBuilder.standard().withCredentials(options.getAwsCredentialsProvider()).withRegion(Regions.AP_SOUTH_1).build(); 
//		
//		ListObjectsV2Result result = s3client.listObjectsV2("aa-ae-coe", "etlpoc/writetos3/");
//		List<S3ObjectSummary> objects = result.getObjectSummaries();
//		objects.remove(0);
//		
//
//        Statement statement = null;
//        try {
//            statement = connection.createStatement();
//            
//            for(S3ObjectSummary os : objects) {
//            	//statement.execute("load data FROM S3 \"" + filePath.replaceAll("\\\\", "/") + "\" into table auroraTestDB.sample LINES TERMINATED BY '\\n'");
//            	statement.addBatch("load data FROM S3 \"" + path.subSequence(0, path.substring(0).lastIndexOf("/")+1) + os.getKey().substring(os.getKey().lastIndexOf("/")+1).replaceAll("\\\\", "/") + "\" into table auroraTestDB.sample LINES TERMINATED BY '\\n'");
//            }            
//            
//            statement.executeBatch();
//            
//            
//        } catch(SQLException e) {
//        	e.printStackTrace();
//        } 
//        finally {
//            if (statement != null) {
//                try {
//                    statement.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        if (connection != null) {
//            try {
//            	
//                connection.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }

		long endTime1 = System.currentTimeMillis();
		System.out.println((endTime1 - beginTime1) / 1000 + " --------*********Extract Load total seconds");
	}
	
	
	@Override
	public String handleRequest(Map<String,String> input, Context context) {
	  
		long beginTime = System.currentTimeMillis();
		
//		AWSCredentials credentials = new BasicAWSCredentials(input.get("inputUrl"), "************************************");
//	    PipelineOptions options = PipelineOptionsFactory.as(AwsOptions.class);
//	    options.setRunner(DirectRunner.class);
//
//	    options.as(AwsOptions.class).setAwsCredentialsProvider(new AWSStaticCredentialsProvider(credentials));
//	    options.as(AwsOptions.class).setAwsRegion("us-east-1");
//	    options.as(AwsOptions.class).setAwsRegion("us-east-2");
		
		/*
		 * SparkSession spark = SparkSession .builder() .appName("JsonETLLambda")
		 * .config("spark.master", "https://aa-ae-coe.s3.ap-south-1.amazonaws.com:7077")
		 * .config("spark.driver.host","192.168.43.84")
		 * .config("spark.driver.bindAddress","192.168.43.84") .getOrCreate();
		 */
		
		String [] args = new String[8];
		
		args[0] = "--runner="+input.get("runner");
		args[1] = "--bucketInputUrl="+input.get("inputUrl");
		args[2] = "--bucketOutputUrl="+input.get("outputUrl");
		args[3] = "--awsAccessKey="+input.get("accessKey");
		args[4] = "--awsSecretKey="+input.get("secretKey");
		args[5] = "--awsRegion=ap-south-1";
		
		
		System.out.println("Options are ----" + args[0] + " 22 " + args[1] + " 33 " + args[2] + " 44 " + args[3] + " 55 " + args[4]);
				
		JsonTrialOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(JsonTrialOptions.class);		
		

		AwsOptionsParser.formatOptions(options);

			try {
				runJsonTransform(options);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		

		long endTime = System.currentTimeMillis();
		return((endTime - beginTime) / 1000 + " --------*********total seconds");
	}

}
