package org.example;

import java.io.File;

public class Main
{
    public static void main(String[] args) throws Exception
    {
        File file = new File("src/main/resources/sample1.txt");

        String bucketName = "sample_bucket";
        String objectKey = file.getName();

        AwsS3Util awsS3Util = new AwsS3Util();
        awsS3Util.setS3client();
        awsS3Util.uploadObject(bucketName, objectKey, file);
        awsS3Util.listOfObjectsInBucket(bucketName);
        awsS3Util.listAllBuckets();
    }
}
