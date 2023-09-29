package org.example;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;

import java.io.*;
import java.util.Date;
import java.util.List;

public class AwsS3Util
{
    String access_key = "your_access_key";
    String secret_key = "your_secret_key";
    String end_point = "https://s3.us-east-1.amazonaws.com";
    AmazonS3 s3client;

    // Call this method before calling the other methods
    public void setS3client() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setProtocol(Protocol.HTTPS);
        final S3ClientOptions options = new S3ClientOptions();
        options.setPathStyleAccess(true);

        AWSCredentials credentials = new BasicAWSCredentials(access_key, secret_key);
        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(clientConfiguration)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(end_point, "us-east-1"))
                .withPathStyleAccessEnabled(true)
                .build();
        this.s3client = s3client;
    }

    //Creates a new bucket if it does not exist
    void createBucket(String bucketName) {
        if(s3client.doesBucketExistV2(bucketName))
        {
            System.out.println("Bucket name is not available."
                    + " Try again with a different Bucket name.");
            return;
        }
        s3client.createBucket(bucketName);
    }

    //Lists all the buckets in the namespace
    void listAllBuckets()
    {
        List<Bucket> buckets = s3client.listBuckets();
        System.out.println("List of exisisting buckets:");
        for(Bucket bucket : buckets) {
            System.out.println(bucket.getName());
        }
    }

    //Deletes an object in a bucket
    void deleteObject(String bucketName, String objectKey) {
        if(!s3client.doesBucketExistV2(bucketName))
        {
            System.out.println("Bucket does not exist.");
            return;
        }
        s3client.deleteObject(new DeleteObjectRequest(bucketName, objectKey));
    }

    //Deletes the bucket if it is empty
    void deleteEmptyBucket(String bucketName) {
        if(!s3client.doesBucketExistV2(bucketName))
        {
            System.out.println("Bucket does not exist.");
            return;
        }
        s3client.deleteBucket(bucketName);
    }

    //Lists out all the objects in a bucket
    void listOfObjectsInBucket(String bucketName) {
        if(!s3client.doesBucketExistV2(bucketName))
        {
            System.out.println("Bucket does not exist.");
            return;
        }
        ListObjectsV2Result result = s3client.listObjectsV2(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for(S3ObjectSummary os: objects)
        {
            System.out.println("Summary: " + os.getKey());
        }
    }

    //Adds an object to the bucket
    void uploadObject(String bucketName, String objectKey, File file) {
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectKey, file);
        s3client.putObject(putObjectRequest);
    }

    //Fetches an object from the bucket
    void downloadObject(String bucketName, String objectKey) throws IOException {
        S3Object s3Object = s3client.getObject(new GetObjectRequest(bucketName, objectKey));
        System.out.println("Content: " + new String(IOUtils.toByteArray(s3Object.getObjectContent())));
    }

    //Deletes all the objects in a bucket
    void deleteObjectsInaBucket(String bucketName) {
        ObjectListing result = s3client.listObjects(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();

        while (result.isTruncated()) {
            result = s3client.listNextBatchOfObjects(result);
            objects.addAll (result.getObjectSummaries());
        }

        System.out.println(bucketName + " " + objects.size());
        for(S3ObjectSummary os: objects)
        {
            deleteObject(bucketName, os.getKey());
        }
    }

    //Deletes all the objects after a specified date
    void deleteObjectsInaBucketAfterDate(String bucketName, Date date) {
        ObjectListing result = s3client.listObjects(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();

        while (result.isTruncated()) {
            result = s3client.listNextBatchOfObjects(result);
            objects.addAll (result.getObjectSummaries());
        }

        System.out.println(bucketName + " " + objects.size());
        int i = 0;
        for(S3ObjectSummary os: objects)
        {
            if(os.getLastModified().after(date))
            {
                i = i+1;
                System.out.println(os.getLastModified());
                deleteObject(bucketName, os.getKey());
            }
        }
        System.out.println("Deleted count: " + i);
    }
}
