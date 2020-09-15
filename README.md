# PDF-Document-Conversion-in-the-Cloud

The application is composed of a local application(client) and instances running on the Amazon cloud. The application gets as an input a text file containing a list of URLs of PDF files with an operation to perform on them in S3. Then,The manager(Ec2 instance) need to handle the request by communicate with the workers using SQS. Each worker will download PDF files from S3, perform the requested operation, and will upload the result of the operation on S3.

The full assignment specification - 
https://www.cs.bgu.ac.il/~dsp202/Assignments/Assignment_1
