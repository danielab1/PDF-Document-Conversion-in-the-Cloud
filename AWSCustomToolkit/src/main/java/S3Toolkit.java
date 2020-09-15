import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;

public class S3Toolkit {
    private static class S3 {
//        private static AWSCredentialsProvider credentialsProvider =
//                new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        private static S3Toolkit toolkit = new S3Toolkit();
        private static final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }

    public static S3Toolkit getToolkit() {
        return S3Toolkit.S3.toolkit;
    }

    public static AmazonS3 getBuilder() {
        return S3Toolkit.S3.s3;
    }

    public boolean CreateBucket(String bucketName) {
        try {
            S3.s3.createBucket(bucketName);
            return true;
        } catch (Exception e) {
            return false;
        }

    }

    public S3Object DownloadFile(String bucketName, String filename, Callback<String> taskHandler) {
        System.out.format("Downloading %s from S3 bucket %s...\n", filename, bucketName);
        S3Object o = null;

        try {
            S3Object object = S3.s3.getObject(new GetObjectRequest(bucketName, filename));

            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    object.getObjectContent()));
            File file = new File("localFilename");
            Writer writer = new OutputStreamWriter(new FileOutputStream(file));

            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;

                taskHandler.call(line + "\n");
            }

            writer.close();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return o;
    }

    public boolean UploadFile(String bucketName, String path, String entryName) { ;
        try {
            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(bucketName, entryName, new File(path));
            S3.s3.putObject(request);
        } catch (SdkClientException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
            return false;
        }
        System.out.printf("File %s was uploaded successfully.\n", entryName);
        return true;
    }

    public String generateUrl(String bucketName, String filename) {
        return "https://" + bucketName + ".s3.amazonaws.com/" + filename;
    }


}
