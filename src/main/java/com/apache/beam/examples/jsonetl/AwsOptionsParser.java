package com.apache.beam.examples.jsonetl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.apache.beam.examples.jsonetl.JsonETLLambda.JsonTrialOptions;

public class AwsOptionsParser {

  // AWS configuration values
  private static final String AWS_DEFAULT_REGION = "ap-south-1";
  private static final String AWS_S3_PREFIX = "s3";

  public static void formatOptions(JsonTrialOptions options) {
    if (options.getBucketOutputUrl().toLowerCase().startsWith(AWS_S3_PREFIX)) {
      setAwsCredentials(options);
    }

    if (options.getAwsRegion() == null) {
      setAwsDefaultRegion(options);
    }
  }

  private static void setAwsCredentials(JsonTrialOptions options) {
    options.setAwsCredentialsProvider(
        new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey())));
  }

  private static void setAwsDefaultRegion(JsonTrialOptions options) {
    if (options.getAwsRegion() == null) {
      options.setAwsRegion(AWS_DEFAULT_REGION);
    }
  }
}
