import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SQSToolkit {
    private static class SQS {
        private static SQSToolkit toolkit = new SQSToolkit();
        private static final AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.EU_CENTRAL_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }
    public static SQSToolkit getToolkit() {
        return SQSToolkit.SQS.toolkit;
    }

    public static AmazonSQS getBuilder() {
        return SQSToolkit.SQS.sqs;
    }

    public void SendMsgToQueue(String queueUrl, String body,String senderId){

        MessageAttributeValue senderIdAttr = new MessageAttributeValue()
                .withDataType("String")
                .withStringValue(senderId);
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(body)
                .addMessageAttributesEntry("senderId", senderIdAttr);
        SQS.sqs.sendMessage(send_msg_request);
    }

    public void SendMsgToQueue(String queueUrl, String body, Map<String, String> attribs, String senderId){

        MessageAttributeValue senderIdAttr = new MessageAttributeValue()
                .withDataType("String")
                .withStringValue(senderId);
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(body)
                .addMessageAttributesEntry("senderId", senderIdAttr);

        for(String key: attribs.keySet()){
            MessageAttributeValue attr = new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(attribs.get(key));
            send_msg_request.addMessageAttributesEntry(key, attr);
        }
        SQS.sqs.sendMessage(send_msg_request);
    }

    public void SendMsgToQueue(String queueUrl, String body, Map<String, MessageAttributeValue> attribs){

        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(body)
                .withMessageAttributes(attribs);

        SQS.sqs.sendMessage(send_msg_request);
    }

    public String getQueueURL(String QUEUE_NAME){
        return SQS.sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
    }
    public String CreateSQS(String QUEUE_NAME,String visibilityTime){
        Map <String,String> atr = new HashMap<>();
        atr.put("VisibilityTimeout",visibilityTime);
        CreateQueueRequest create_request = new CreateQueueRequest(QUEUE_NAME).withAttributes(atr);

        try {
            SQS.sqs.createQueue(create_request);
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
        System.out.printf("SQS %s was created successfully.\n", QUEUE_NAME);
        String queueUrl = SQS.sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
//        sqs.changeMessageVisibility(queueUrl, 0);
        return queueUrl;
    }

    public static void changeMessageVisibilitySingle(String queue_url, int timeout) {
        // Get the receipt handle for the first message in the queue.
        String receipt = SQS.sqs.receiveMessage(queue_url)
                .getMessages()
                .get(0)
                .getReceiptHandle();

        SQS.sqs.changeMessageVisibility(queue_url, receipt, timeout);
    }

    public void deleteMsgFromQueue(String queueUrl, Message m){
        DeleteMessageRequest request = new DeleteMessageRequest(queueUrl, m.getReceiptHandle());
        SQS.sqs.deleteMessage(request);
    }

    public List<Message> getQueueMessages(String queueUrl){
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl)
                .withMessageAttributeNames("All")
                .withMaxNumberOfMessages(10);
        return SQS.sqs.receiveMessage(request).getMessages();
    }
    public int getNumOfMessages(String queueUrl){
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add("All");
        // list the attributes of the queue we are interested in
        GetQueueAttributesRequest request = new GetQueueAttributesRequest(queueUrl);
        request.setAttributeNames(attributeNames);
        Map<String, String> attributes = SQS.sqs.getQueueAttributes(request)
                .getAttributes();
        int notVisibleMsg = Integer.parseInt(attributes
                .get("ApproximateNumberOfMessagesNotVisible"));
        int visibleMsg = Integer.parseInt(attributes
                .get("ApproximateNumberOfMessages"));
        return notVisibleMsg + visibleMsg;
    }
    public void deleteQueue(String queueUrl){
        SQS.sqs.deleteQueue(queueUrl);
    }

    public void setVisibility(String queueUrl,String receipt,int timeout){
        SQS.sqs.changeMessageVisibility(queueUrl, receipt, timeout);
    }
}
