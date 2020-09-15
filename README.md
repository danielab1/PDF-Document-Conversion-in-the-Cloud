# PDF-Document-Conversion-in-the-Cloud

The application is composed of a local application(client) and instances running on the Amazon cloud. The application gets as an input a text file containing a list of URLs of PDF files with an operation to perform on them in S3. Then,The manager(Ec2 instance) need to handle the request using the workers. Each worker will download PDF files, perform the requested operation, and display the result of the operation on a webpage.

The full assignment specification - 
https://www.cs.bgu.ac.il/~dsp202/Assignments/Assignment_1
