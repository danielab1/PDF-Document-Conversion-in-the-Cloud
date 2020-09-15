import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.util.EC2MetadataUtils;
import com.sun.org.apache.xml.internal.security.utils.Base64;


public class EC2Toolkit {
    private PropertiesReader prop = new PropertiesReader(System.getProperty("user.dir")+"/configure.properties");

//    private static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
    private static class EC2 {
        private static EC2Toolkit toolkit = new EC2Toolkit();
        private static final AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }

    public static EC2Toolkit getToolkit() {
        return EC2.toolkit;
    }

    public static AmazonEC2 getBuilder() {
        return EC2.ec2;
    }

    public void CreateEC2Instance(String amiId) {
        String tagKey = amiId.equals("MANAGER_AMI") ? "Manager" : "Worker";
        Tag ec2Tag = new Tag(tagKey, tagKey);
        TagSpecification ts = new TagSpecification();
        ts.withTags(ec2Tag).setResourceType(ResourceType.Instance);

        RunInstancesRequest run_request = new RunInstancesRequest()
                .withImageId(prop.getProperty(amiId))
                .withInstanceType(InstanceType.T2Micro)
                .withMaxCount(1)
                .withMinCount(1)
                .withTagSpecifications(ts)
//                .withKeyName("danielsag")
                .withKeyName("assignment_dsps")
                .withUserData(defineEnvVariable(tagKey))
                .withIamInstanceProfile(new IamInstanceProfileSpecification().withArn("arn:aws:iam::839580361805:instance-profile/managerIam"));

        RunInstancesResult run_response = EC2.ec2.runInstances(run_request);

        String reservation_id = run_response.getReservation().getInstances().get(0).getInstanceId();
    }

    public boolean isInstanceRunning(String tagName) {
        DescribeInstancesRequest request = new DescribeInstancesRequest();

        DescribeInstancesResult response = EC2.ec2.describeInstances(request);

        for (Reservation reservation : response.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                //TODO: is there any more efficient way than iterating on all instances
                String state =  instance.getState().getName();
                boolean isRunning = state.equals("running") || state.equals("pending");
                if (isRunning && instance.getTags().contains(new Tag(tagName, tagName))) {
                    return true;
                }
            }
        }
        if(tagName.equals("Manager"))
             System.out.println("Manager offline");
        return false;
    }


    public void terminatedInstance(String idInstance) {
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
                .withInstanceIds(idInstance);
        EC2.ec2.terminateInstances(terminateInstancesRequest)
                .getTerminatingInstances()
                .get(0)
                .getPreviousState()
                .getName();
    }


    public String getInstanceId(){
        return EC2MetadataUtils.getInstanceId();
    }

    private String defineEnvVariable(String jarName){

        String[] s = {"#!/bin/bash",
                    "cd //home/ec2-user",
                    "aws s3 cp s3://dsp-ass1-jars/aws-setup.sh setup.sh",
                    "sh setup.sh",
                    "java -jar "+jarName+".jar"};

        byte[] bytes = String.join("\n",s).getBytes();
        return Base64.encode(bytes);
    }

    public int getAmountOfInstanceByTag(String tagName) {
        DescribeInstancesRequest request = new DescribeInstancesRequest()
                .withFilters(new Filter("instance-state-name").withValues("running", "pending"),
                new Filter("tag-key").withValues(tagName));

        DescribeInstancesResult response = EC2.ec2.describeInstances(request);
        int count = 0;
        for (Reservation reservation : response.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                    count++;
            }
        }
        return count;
    }
}


