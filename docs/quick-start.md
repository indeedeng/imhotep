---
layout: default
title: Quick Start
permalink: /docs/quick-start/
---

<div style="color: #000000; background-color: #f5f5f5; border-color: #f5f5f5; padding: 15px; border: 1px solid transparent; border-radius: 4px; font-size: 16px; margin-top: 20px; margin-bottom: 20px;">This Quick Start guides you through the process of configuring an Imhotep cluster on AWS, uploading your data files, and exploring your data in Imhotep using Imhotep Query Language (IQL).</div>

<div style="color: #000000; background-color: #f5f5f5; border-color: #f5f5f5; padding: 15px; border: 1px solid transparent; border-radius: 4px; font-size: 16px; margin-top: 20px; margin-bottom: 20px;">If you prefer to run an evaluation Imhotep stack using Docker, follow the instructions for <a href="http://agrover-indeed.indeedeng.io/imhotep/docs/evaluate-docker">Evaluating Imhotep with Docker</a>.</div>

<div style="color: #000000; background-color: #f5f5f5; border-color: #f5f5f5; padding: 15px; border: 1px solid transparent; border-radius: 4px; font-size: 16px; margin-top: 20px; margin-bottom: 20px;">You can also manually install and configure Imhotep on AWS without CloudFormation scripts using <a href="http://agrover-indeed.indeedeng.io/imhotep/docs/aws"> Installing Imhotep on AWS</a> documentation.</div>


## Table of Contents

* [Create a Cluster on AWS](#creating-a-cluster-on-aws)
* [Make Data Available in Imhotep](#imhotep-tsv-uploader)
* [Query your Imhotep Cluster using IQL](#iql-web-client)

<sub>Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)</sub>


## Creating a Cluster on AWS

### Prerequisites
You will use AWS CloudFormation, Amazon S3 and Amazon EC2 to create your Imhotep cluster on AWS.
<ol>
  <li>If you don't already have an <a href="http://aws.amazon.com/">AWS account</a>, create one. Allow time for your new account to be verified before you attempt to create the stack. Account verification might take several hours.</li>
  <li>Sign in to the AWS Management Console and navigate to <strong>S3 > Create Bucket</strong> to create the following buckets:<br><br></li>
<ul>
<li>a build bucket to store your uploaded data</li>
<li>a data bucket to store your Imhotep datasets</li>
</ul>
<li>From the AWS Management Console in <strong>EC2 > Key Pairs > Create Key Pair</strong>, create public and private keys.</li>
</ol>

<strong>IMPORTANT</strong>: Running Imhotep on AWS will incur costs to your AWS account. We recommend shutting down your Imhotep cluster by deleting the CloudFormation stack when it is not in use. At a minimum, for a small amount of data, you will be running two r3.large instances and one r3.xlarge instance. Additional costs will be incurred for data upload and instance provisioning. For larger data sizes, you might need to increase the AWS resources and therefore costs. <a href="http://aws.amazon.com/ec2/pricing/">Click here for information about AWS costs</a>.

Deleting the stack does not delete data you have uploaded to your S3 buckets. When you are ready for a new session, recreate the stack and point to the existing S3 buckets.


### Setup
Use AWS CloudFormation to create a stack on AWS.
<ol>
  <li>Select <strong>CloudFormation</strong> and <strong>Create Stack</strong>.</li>
  <li>From the <strong>Select Template</strong> page, enter the name of your new stack.</li>
  <li>From this same page, in <strong>Template</strong>, select <strong>Specify an Amazon S3 template URL</strong> and enter this URL:<br><br>
  <pre>http://indeedeng-imhotep-build.s3.amazonaws.com/cloudFormation_cluster_launch.json</pre></li>
  <li>Click <strong>Next</strong> to define the following parameters:
 <br><br></li>
<table>
  <thead>
  <th>Parameter</th>
  <th>Description</th>
  </thead>
  <tbody>
   <tr>
    <td valign="top"><code>BuildBucket</code></td>
    <td valign="top">Name of the bucket you created to store your uploaded data. Ensure that the name you enter matches the name of the build bucket you created.</td>
  </tr>
    <tr>
    <td valign="top"><code>DownloadBucket</code></td>
    <td valign="top">The predefined <code>indeed-imhotep-downloads</code>bucket that contains Imhotep jars. Do not rename this parameter (unless you are building your own Imhotep images).</td>
  </tr>
  <tr>
    <td valign="top"><code>DataBucket</code></td>
    <td valign="top">Name of the bucket you created for your Imhotep datasets. Ensure that the name you enter matches the name of the data bucket you created.</td>
  </tr>
<tr>
    <td valign="top"><code>InstanceType</code></td>
    <td valign="top">Defines the memory, CPU, storage capacity, and hourly cost for the Imhotep instance. Valid values include:<br> <code>m3.xlarge</code><br><code>m3.2xlarge</code><br><code>c3.2xlarge</code><br><code>c3.4xlarge</code><br><code>c3.8xlarge</code><br><code>r3.large</code> (smallest and least expensive)<br><code>r3.xlarge</code><br><code>r3.2xlarge</code><br><code>r3.4xlarge</code><br><code>r3.8xlarge</code><br><a href="http://aws.amazon.com/ec2/pricing/">Click here for information about instance costs</a>.</td>
  </tr>
  <tr>
    <td valign="top"><code>IPrange</code></td>
     <td valign="top">IP address range for web access to Imhotep. The range must be a valid IP <a href="http://searchnetworking.techtarget.com/definition/CIDR">Classless Inter-Domain Routing</a> (CIDR) range of the form <code>x.x.x.x/x</code><br><br>Any traffic coming from outside of this range won't be able to access Imhotep. You can change the IP address range later by deleting and recreating your stack. If you know <a href="http://www.myipaddress.com/show-my-ip-address/">your IP address</a>, use <code>myIPAddress/0</code></td>
  </tr>
  <tr>
    <td valign="top"><code>KeyName</code></td>
    <td valign="top">Name of an existing EC2 key pair to enable SSH access to the cluster. Create your <code>KeyName</code> in the same region as the Key Pair you created as a prerequisite to this procedure. </td>
  </tr>
    <tr>
    <td valign="top"><code>LoginId</code></td>
    <td valign="top">Your user ID for logging into Imhotep. </td>
  </tr>
  <tr>
    <td valign="top"><code>LoginPassword</code></td>
    <td valign="top">Your password for logging into Imhotep. </td>
  </tr>
  <tr>
    <td valign="top"><code>NumImhotepInstances</code></td>
    <td valign="top">Number of Imhotep instances in the cluster that service queries. The default value is 2. Increase this number for greater scalability.</td>
  </tr>
  <tr>
    <td valign="top"><code>SSHLocation</code></td>
    <td valign="top">IP address range for SSH access to the cluster. The range must be a valid IP <a href="http://searchnetworking.techtarget.com/definition/CIDR">CIDR</a> range of the form <code>x.x.x.x/x</code><br><br>Any SSH traffic coming from outside of this range won't be able to access Imhotep. You can change the IP address range later by deleting and recreating your stack.</td>
  </tr>
  </tbody>
</table>

  <li>Click <strong>Next</strong> through the remaining options of the stack setup until you see a <strong>Review</strong> page with the options you defined.</li>
  <li>Allow the template to create IAM resources: from the <strong>Review</strong> page, scroll down to the <strong>Capabilities</strong> section and select the acknowledgment.</li>
  <li>Click <strong>Create</strong>. </li>
  </ol>
  
The process might take several minutes. When the setup is successful, URLs are available on the <strong>Outputs</strong> tab for Imhotep TSV Uploader and the IQL web client. Allow several minutes for the services to become available.

* TSV Uploader allows you to upload your data to Imhotep. 
* The IQL web client allows you to query the Imhotep cluster using IQL queries. [Learn about IQL.]({{ site.baseurl }}/docs/overview)

## Imhotep TSV Uploader

Use TSV Uploader to make your data available in Imhotep. TSV Uploader converts your data files into datasets that Imhotep can use and moves the datasets to the correct location so that Imhotep can access them. 

[Learn about data file requirements for TSV Uploader]({{ site.baseurl }}/docs/data-file-requirements).

### Logging into TSV Uploader
1. Open a browser and navigate to the Imhotep TSV Uploader URL provided when you created the stack on AWS.
2. Bypass the SSL warning to reach the login screen.
3. Enter your login ID and password that you defined during setup.

### Creating a Dataset
1. Log into TSV Uploader. 
2. Scroll to the bottom of list of available datasets and enter a name for your new dataset in the text entry box. The dataset name must be at least two characters long and can contain only lowercase `a-z` and digits.
2. Click <strong>+</strong> to create the dataset.

The name of your new dataset appears in the list. When you first add the dataset, it is empty until you upload a data file. A dataset is not created on Imhotep until you upload a data file and a shard is created.

### Uploading a Data File
To test your stack, consider uploading the sample time-series dataset in [nasa_19950801.tsv](http://indeedeng.github.io/imhotep/files/nasa_19950801.tsv). For more information about this dataset, [click here](../sample-data#nasa-apache-web-logs/).

1. Log into TSV Uploader and click the dataset name.
2. In the search field near the top of the page, click <strong>Upload TSV</strong> and browse to the TSV file that contains your data. Repeat this step to upload additional data files to your dataset. To upload multiple files at one time, with the dataset name selected, drag and drop the files to the TSV Uploader window.
3. Refresh the page to show the status of the upload.

When the process completes successfully, <code>indexed</code> shows as the status of the file. Allow a minute or two for your dataset to be available in the IQL web client. 

If the process fails, <code>failed</code> shows as the status. Errors are written to a <code>.error.log</code> file, which you can download to your computer. 

To upload files directly to your S3 build bucket, place the files in the <strong>iupload/tsvtoindex/<em>datasetName</em>/</strong> directory. As they are processed, they are moved to <strong>iupload/indexedtsv/<em>datasetName</em>/</strong>. You can also view the files in TSV Uploader.

NOTE: If you upload a TSV file to the wrong dataset, you must manually remove the shard that contains the dataset from Imhotep. [Learn how]({{ site.baseurl }}/docs/troubleshooting). 

To download a data file to your computer, select <em>datasetName</em> <strong>></strong> <em>dataFileName</em> and click the download button in <strong>Operations</strong>. 


### Deleting Files from TSV Uploader
To delete a data file, select <em>datasetName</em> <strong>></strong> <em>dataFileName</em> and click the trash can. 

To delete a dataset, select <em>datasetName</em> and click the trash can.

NOTE: Deleting a data file or dataset from TSV Uploader does not delete the dataset from Imhotep. TSV Uploader shows the list of data files for two weeks after a file's upload date.

## IQL Web Client
Use the IQL web client to query the Imhotep cluster using IQL. 

Logging into the client:

1. Open a browser and navigate to the IQL URL provided when you created the stack on AWS.
2. Bypass the SSL warning to reach the login screen.
3. Enter your login ID and password.

Follow these general steps to construct an IQL query:

1. Formulate your question.
2. Select your dataset and the date range.
3. Enter the query.
4. Select how you want to group your data. Groups show as rows in tabular data.
5. Choose one or multiple metrics for your data. Metrics show as columns in tabular data.
6. Run your query.

[Learn more about using IQL]({{ site.baseurl }}/docs/overview).
