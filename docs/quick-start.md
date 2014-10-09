---
layout: default
title: Quick Start
permalink: /docs/quick-start/
---

This section guides you through the process of configuring an Imhotep cluster on AWS, uploading your data, and accessing your data in Imhotep using Imhotep Query Language (IQL).

##Creating a Cluster on AWS

###Prerequisites
You must have an AWS account and three S3 buckets for data storage. For this configuration, you will use AWS CloudFormation, Amazon S3 and Amazon EC2.

###Setup
Use AWS CloudFormation to create a stack on AWS.
<ol>
  <li>Sign in to the AWS Management Console. </li>
  <li>Select **CloudFormation** and **Create Stack**.</li>
  <li>From the **Select Template** page, enter the name of your new stack.</li>
  <li>From this same page, in **Template**, select **Specify an Amazon S3 template URL** and enter this URL: 
  `http://imhotep-cloud-setup.s3.amazonaws.com/cloudFormation_cluster_launch.json`</li>
  <li>Click **Next** to define the following parameters:
 <br><br></li>
<table>
  <thead>
  <th>Parameter</th>
  <th>Description</th>
  </thead>
  <tbody>
  <tr>
    <td valign="top">`InstanceType`</td>
    <td valign="top">Defines the memory, CPU, storage capacity, and hourly cost for the Imhotep instance. Valid values include:<br> `m3.xlarge`<br>`m3.2xlarge`<br>`c3.2xlarge`<br>`c3.4xlarge`<br>`c3.8xlarge`<br>`r3.large`<br>`r3.xlarge`<br>`r3.2xlarge`<br>`r3.4xlarge`<br>`r3.8xlarge`</td>
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
    <td valign="top">By default, the setup script creates two Imhotep instances for scalability.</td>
  </tr>
  <tr>
    <td valign="top">`s3BuildBucket`</td>
    <td valign="top">Name of the bucket you created for your data from TSV Uploader. Ensure that the name you enter matches the name of the build bucket you created.</td>
  </tr>
  <tr>
    <td valign="top">`s3cacheBucket`</td>
    <td valign="top">Name of the bucket you created for your cached results from IQL queries. Ensure that the name you enter matches the name of the cache bucket you created.</td>
  </tr>
  <tr>
    <td valign="top">`s3dataBucket`</td>
    <td valign="top">Name of the bucket you created for your Imhotep indexes. Ensure that the name you enter matches the name of the data bucket you created.</td>
  </tr>
  <tr>
    <td valign="top">`s3imhotepBucket`</td>
    <td valign="top">The predefined bucket that contains Imhotep jars. Do not rename this parameter (unless you are building your own Imhotep images).</td>
  </tr>
  <tr>
    <td valign="top">`s3Key`</td>
    <td valign="top">Your key for bucket access.</td>
  </tr>
  <tr>
    <td valign="top">`s3Secret`</td>
    <td valign="top">Your S3 secret to use with the s3Key for bucket access.</td>
  </tr>
  <tr>
    <td valign="top">`SSHLocation`</td>
    <td valign="top">IP address range for SSH access to the cluster. The range must be a valid IP CIDR range of the form `x.x.x.x/x`</td>
  </tr>
  </tbody>
</table>

  <li>Click **Next** through the remaining options of the setup script until you see a review page with the options you defined.</li>
  <li>Click **Create**. </li>
  </ol>
  
The process might take several minutes. When the setup is successful, URLs are available on the **Outputs** tab for Imhotep TSV Uploader and the IQL web client.

* TSV Uploader allows you to upload your data to Imhotep. 
* The IQL web client allows you to query the Imhotep cluster using IQL queries. [Learn about IQL.]({{ site.baseurl }}/docs/overview)

## Imhotep TSV Uploader

Use TSV Uploader to make your data available in Imhotep. TSV Uploader converts your data files into indexes that Imhotep can use and moves the indexes to the correct location so that Imhotep can access them. 

[Learn about data file requirements for TSV Uploader]({{ site.baseurl }}/docs/data-file-requirements).

### Creating an Index
1. Open a browser and navigate to the Imhotep TSV Uploader URL provided when you created the stack on AWS. A list of indexes that are available in the system appears on the left side of the page. 
2. Scroll to the bottom of this list and enter a name for your new index in the text entry box. Use lowercase characters matching regex [a-z0-9]+ for your index name.
3. Click **+** to create the index.

The name of your new index appears in the list. When you first add the index, it is empty until you upload a data file. An index is not created on Imhotep until you upload a data file and a shard is created.

### Uploading a Data File
1. In Imhotep TSV Uploader, click the index name.
2. In the search field near the top of the page, click **Upload TSV** and browse to the TSV file that contains your index data. Repeat this step to upload additional data files to your index. To upload multiple files at one time, with the index name selected, drag and drop the files to the TSV Uploader window.

NOTE: If you upload a TSV file to the wrong index, you must manually remove the shard that contains the index from Imhotep. [Learn how]({{ site.baseurl }}/docs/troubleshooting). 

When the process completes successfully, `indexed` shows as the status of the file. If the process fails, `failed` shows as the status. Errors are written to a `.error.log` file, which you can download to your computer. 

To download a data file to your computer, select *indexName* **>** *dataFileName* and click the download button in **Operations**. 


### Deleting Files from TSV Uploader
To delete a data file, select *indexName* **>** *dataFileName* and click the trash can. 

To delete an index, select *indexName* and click the trash can.

NOTE: Deleting a data file or index from TSV Uploader does not delete the index from Imhotep. TSV Uploader shows the list of data files for two weeks after a file's upload date.

##IQL Web Client
Use the IQL web client to query the Imhotep cluster using Imhotep query language (IQL). To launch the client, open a browser and navigate to the IQL URL provided when you created the cluster on AWS. Constructing an IQL query follows these general steps:

1. Formulate your question.
2. Select your index and the date range.
3. Enter the query.
4. Select how you want to group your data. Groups show as rows in tabular data.
5. Choose one or multiple metrics for your data. Metrics show as columns in tabular data.
6. Run your query.

[Learn more about using the IQL web client]({{ site.baseurl }}/docs/overview).
