---
layout: default
title: Quick Start
permalink: /docs/quick-start/
---

This section guides you through the process of configuring an Imhotep cluster on AWS, uploading your data files, and exploring your data in Imhotep using Imhotep Query Language (IQL).

##Creating a Cluster on AWS

###Prerequisites
You will use AWS CloudFormation, Amazon S3 and Amazon EC2 to create your Imhotep cluster on AWS.
<ol>
  <li>If you don't already have an [AWS account](http://aws.amazon.com/), create one. Allow time for your new account to be verified before you attempt to create the stack. Account verification might take several hours.</li>
  <li>Sign in to the AWS Management Console and navigate to **S3 > Create Bucket** to create the following buckets:<br><br></li>
<ul>
<li>a build bucket to store your uploaded data</li>
<li>a data bucket to store your Imhotep datasets</li>
</ul>
<li>From the AWS Management Console in **EC2 > Key Pairs > Create Key Pair**, create public and private keys.</li>
</ol>

IMPORTANT: Running Imhotep on AWS will incur costs to your AWS account. We recommend shutting down your Imhotep cluster by deleting the CloudFormation stack when it is not in use. At a minimum, for a small amount of data, you will be running two r3.large instances and one r3.xlarge instance. Additional costs will be incurred for data upload and instance provisioning. For larger data sizes, you might need to increase the AWS resources and therefore costs. [Click here for information about AWS costs](http://aws.amazon.com/ec2/pricing/).

Deleting the stack does not delete data you have uploaded to your S3 buckets. When you are ready for a new session, recreate the stack and point to the existing S3 buckets.


###Setup
Use AWS CloudFormation to create a stack on AWS.
<ol>
  <li>Sign in to the AWS Management Console. </li>
  <li>Select **CloudFormation** and **Create Stack**.</li>
  <li>From the **Select Template** page, enter the name of your new stack.</li>
  <li>From this same page, in **Template**, select **Specify an Amazon S3 template URL** and enter this URL:<br><br>
  <pre>http://imhotep-cloud-setup.s3.amazonaws.com/cloudFormation_cluster_launch.json</pre></li>
  <li>Click **Next** to define the following parameters:
 <br><br></li>
<table>
  <thead>
  <th>Parameter</th>
  <th>Description</th>
  </thead>
  <tbody>
   <tr>
    <td valign="top">`BuildBucket`</td>
    <td valign="top">Name of the bucket you created to store your uploaded data. Ensure that the name you enter matches the name of the build bucket you created.</td>
  </tr>
    <tr>
    <td valign="top">`DownloadBucket`</td>
    <td valign="top">The predefined `indeed-imhotep-downloads` bucket that contains Imhotep jars. Do not rename this parameter (unless you are building your own Imhotep images).</td>
  </tr>
  <tr>
    <td valign="top">`DataBucket`</td>
    <td valign="top">Name of the bucket you created for your Imhotep datasets. Ensure that the name you enter matches the name of the data bucket you created.</td>
  </tr>
<tr>
    <td valign="top">`InstanceType`</td>
    <td valign="top">Defines the memory, CPU, storage capacity, and hourly cost for the Imhotep instance. Valid values include:<br> `m3.xlarge`<br>`m3.2xlarge`<br>`c3.2xlarge`<br>`c3.4xlarge`<br>`c3.8xlarge`<br>`r3.large`<br>`r3.xlarge`<br>`r3.2xlarge`<br>`r3.4xlarge`<br>`r3.8xlarge`<br>[Click here for information about instance costs](http://aws.amazon.com/ec2/pricing/).</td>
  </tr>
  <tr>
    <td valign="top">`IPrange`</td>
     <td valign="top">IP address range for web access to Imhotep. The range must be a valid IP CIDR range of the form `x.x.x.x/x`</td>
  </tr>
  <tr>
    <td valign="top">`KeyName`</td>
    <td valign="top">Name of an existing EC2 key pair to enable SSH access to the cluster.</td>
  </tr>
  <tr>
    <td valign="top">`NumImhotepInstances`</td>
    <td valign="top">Number of Imhotep instances in the cluster that service queries. The default value is 2. Increase this number for greater scalability.</td>
  </tr>
  <tr>
    <td valign="top">`SSHLocation`</td>
    <td valign="top">IP address range for SSH access to the cluster. The range must be a valid IP CIDR range of the form `x.x.x.x/x`</td>
  </tr>
  </tbody>
</table>

  <li>Click **Next** through the remaining options of the stack setup until you see a **Review** page with the options you defined.</li>
  <li>Allow the template to create IAM resources: from the **Review** page, scroll down to the **Capabilities** section and select the acknowledgment.</li>
  <li>Click **Create**. </li>
  </ol>
  
The process might take several minutes. When the setup is successful, URLs are available on the **Outputs** tab for Imhotep TSV Uploader and the IQL web client. Allow several minutes for the services to become available.

* TSV Uploader allows you to upload your data to Imhotep. 
* The IQL web client allows you to query the Imhotep cluster using IQL queries. [Learn about IQL.]({{ site.baseurl }}/docs/overview)

## Imhotep TSV Uploader

Use TSV Uploader to make your data available in Imhotep. TSV Uploader converts your data files into datasets that Imhotep can use and moves the datasets to the correct location so that Imhotep can access them. 

[Learn about data file requirements for TSV Uploader]({{ site.baseurl }}/docs/data-file-requirements).

### Creating a Dataset
1. Open a browser and navigate to the Imhotep TSV Uploader URL provided when you created the stack on AWS. A list of datasets that are available in the system appears on the left side of the page. 
2. Scroll to the bottom of this list and enter a name for your new dataset in the text entry box. The dataset name must be at least two characters long and can contain only lowercase `a-z` and digits.
3. Click **+** to create the dataset.

The name of your new dataset appears in the list. When you first add the dataset, it is empty until you upload a data file. A dataset is not created on Imhotep until you upload a data file and a shard is created.

### Uploading a Data File
1. In Imhotep TSV Uploader, click the dataset name.
2. In the search field near the top of the page, click **Upload TSV** and browse to the TSV file that contains your data. Repeat this step to upload additional data files to your dataset. To upload multiple files at one time, with the dataset name selected, drag and drop the files to the TSV Uploader window.
3. Refresh the page to show the status of the upload.

When the process completes successfully, `indexed` shows as the status of the file. Allow a minute or two for your dataset to be available in the IQL web client. 

If the process fails, `failed` shows as the status. Errors are written to a `.error.log` file, which you can download to your computer. 

To upload files directly to your S3 build bucket, place the files in the **iupload/tsvtoindex/*datasetName*/** directory. As they are processed, they are moved to **iupload/indexedtsv/*datasetName*/**. You can also view the files in TSV Uploader.

NOTE: If you upload a TSV file to the wrong dataset, you must manually remove the shard that contains the dataset from Imhotep. [Learn how]({{ site.baseurl }}/docs/troubleshooting). 

To download a data file to your computer, select *datasetName* **>** *dataFileName* and click the download button in **Operations**. 


### Deleting Files from TSV Uploader
To delete a data file, select *datasetName* **>** *dataFileName* and click the trash can. 

To delete a dataset, select *datasetName* and click the trash can.

NOTE: Deleting a data file or dataset from TSV Uploader does not delete the dataset from Imhotep. TSV Uploader shows the list of data files for two weeks after a file's upload date.

##IQL Web Client
Use the IQL web client to query the Imhotep cluster using IQL. To launch the client, open a browser and navigate to the IQL URL provided when you created the cluster on AWS. Constructing an IQL query follows these general steps:

1. Formulate your question.
2. Select your dataset and the date range.
3. Enter the query.
4. Select how you want to group your data. Groups show as rows in tabular data.
5. Choose one or multiple metrics for your data. Metrics show as columns in tabular data.
6. Run your query.

[Learn more about using the IQL web client]({{ site.baseurl }}/docs/overview).
