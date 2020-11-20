# Microsoft Azure Storage Data Movement Library (2.0.1)

The Microsoft Azure Storage Data Movement Library designed for high-performance uploading, downloading and copying Azure Storage Blob and File. This library is based on the core data movement framework that powers [AzCopy](https://azure.microsoft.com/documentation/articles/storage-use-azcopy/).

For more information about the Azure Storage, please visit [Microsoft Azure Storage Documentation](https://azure.microsoft.com/documentation/services/storage/).

> Note:
> As of 0.11.0, the namespace has changed to Microsoft.Azure.Storage.DataMovement from Microsoft.WindowsAzure.Storage.DataMovement. 

# Features

- Blobs
    - Download/Upload/Copy Blobs.
    - Copy Blobs with synchronous copying, service side asynchronous copying and service side synchronous copying.
    - Concurrently transfer Blobs and Blob chunks, define number of concurrent operations
    - Download Specific Blob Snapshot

- Files
	- Download/Upload/Copy Files.
    - Copy File with synchronous copying and service side asynchronous copying.
    - Concurrently transfer Files and File ranges, define number of concurrent operations

- General
	- Track data transfer progress
	- Recover the data transfer
	- Set Access Condition
	- Set User Agent Suffix
	- Directory/recursive transfer

# Getting started

For the best development experience, we recommend that developers use the official Microsoft NuGet packages for libraries. NuGet packages are regularly updated with new functionality and hotfixes.

## Target Frameworks

- .NET Framework 4.5.2 or above
- Netstandard2.0

## Requirements

To call Azure services, you must first have an Azure subscription. Sign up for a [free trial](/en-us/pricing/free-trial/) or use your [MSDN subscriber benefits](/en-us/pricing/member-offers/msdn-benefits-details/).


## Download & Install


### Via Git

To get the source code of the SDK via git just type:

```bash
git clone https://github.com/Azure/azure-storage-net-data-movement.git
cd azure-storage-net-data-movement
```

### Via NuGet

To get the binaries of this library as distributed by Microsoft, ready for use
within your project you can also have them installed by the .NET package manager [NuGet](https://www.nuget.org/packages/Microsoft.Azure.Storage.DataMovement).

`Install-Package Microsoft.Azure.Storage.DataMovement`


## Dependencies

### Azure Storage Blob Client Library

This version depends on Azure Storage Blob Client Library

- [Microsoft.Azure.Storage.Blob](https://www.nuget.org/packages/Microsoft.Azure.Storage.Blob/)

### Azure Storage File Client Library

This version depends on Azure Storage File Client Library

- [Microsoft.Azure.Storage.File](https://www.nuget.org/packages/Microsoft.Azure.Storage.File/)


## Code Samples

Find more samples at the [sample folder](https://github.com/Azure/azure-storage-net-data-movement/tree/master/samples?).

### Upload a blob

First, include the classes you need, here we include Storage client library, the Storage data movement library and the .NET threading because data movement library provides Task Asynchronous interfaces to transfer storage objects:

```csharp
using System;
using System.Threading;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement;
```

Now use the interfaces provided by Storage client lib to setup the storage context (find more details at [how to use Blob Storage from .NET](https://azure.microsoft.com/documentation/articles/storage-dotnet-how-to-use-blobs/)):

```csharp
string storageConnectionString = "myStorageConnectionString";
CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);
CloudBlobClient blobClient = account.CreateCloudBlobClient();
CloudBlobContainer blobContainer = blobClient.GetContainerReference("mycontainer");
blobContainer.CreateIfNotExists();
string sourcePath = "path\\to\\test.txt";
CloudBlockBlob destBlob = blobContainer.GetBlockBlobReference("myblob");
```

Once you setup the storage blob context, you can start to use `WindowsAzure.Storage.DataMovement.TransferManager` to upload the blob and track the upload progress,

```csharp
// Setup the number of the concurrent operations
TransferManager.Configurations.ParallelOperations = 64;
// Setup the transfer context and track the upload progress
SingleTransferContext context = new SingleTransferContext();
context.ProgressHandler = new Progress<TransferStatus>((progress) =>
{
	Console.WriteLine("Bytes uploaded: {0}", progress.BytesTransferred);
});
// Upload a local blob
var task = TransferManager.UploadAsync(
	sourcePath, destBlob, null, context, CancellationToken.None);
task.Wait();
```

### Copy a blob

First, include the classes you need, which is the same as the sample to Upload a blob.

```csharp
using System;
using System.Threading;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement;
```

Now use the interfaces provided by Storage client lib to setup the storage contexts (find more details at [how to use Blob Storage from .NET](https://azure.microsoft.com/documentation/articles/storage-dotnet-how-to-use-blobs/)):

```csharp
string sourceStorageConnectionString = "sourceStorageConnectionString";
CloudStorageAccount sourceAccount = CloudStorageAccount.Parse(sourceStorageConnectionString);
CloudBlobClient sourceBlobClient = sourceAccount.CreateCloudBlobClient();
CloudBlobContainer sourceBlobContainer = sourceBlobClient.GetContainerReference("sourcecontainer");
CloudBlockBlob sourceBlob = sourceBlobContainer.GetBlockBlobReference("sourceBlobName");

string destStorageConnectionString = "destinationStorageConnectionString";
CloudStorageAccount destAccount = CloudStorageAccount.Parse(destStorageConnectionString);
CloudBlobClient destBlobClient = destAccount.CreateCloudBlobClient();
CloudBlobContainer destBlobContainer = destBlobClient.GetContainerReference("destinationcontainer");
CloudBlockBlob destBlob = destBlobContainer.GetBlockBlobReference("destBlobName");
```

Once you setup the storage blob contexts, you can start to use `WindowsAzure.Storage.DataMovement.TransferManager` to copy the blob and track the copy progress:

```csharp
// Setup the number of the concurrent operations
TransferManager.Configurations.ParallelOperations = 64;
// Setup the transfer context and track the copy progress
SingleTransferContext context = new SingleTransferContext();
context.ProgressHandler = new Progress<TransferStatus>((progress) =>
{
	Console.WriteLine("Bytes Copied: {0}", progress.BytesTransferred);
});

// Copy a blob
var task = TransferManager.CopyAsync(
    sourceBlob, destBlob, CopyMethod.ServiceSideSyncCopy, null, context, CancellationToken.None);
task.Wait();
```

DMLib supports three different copying methods: Synchronous Copy, Service Side Asynchronous Copy and Service Side Synchronous Copy. The above sample uses Service Side Synchronous Copy. See [Choose Copy Method](#choose-copy-method) for details on how to choose the copy method. 

# Best Practice

### Increase .NET HTTP connections limit
By default, the .Net HTTP connection limit is 2. This implies that only two concurrent connections can be maintained. It prevents more parallel connections accessing Azure blob storage from your application.

AzCopy will set ServicePointManager.DefaultConnectionLimit to the number of eight multiple the core number by default. To have a comparable performance when using Data Movement Library alone, we recommend you set this value as well.

```csharp
ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
```

### Turn off 100-continue
When the property "Expect100Continue" is set to true, client requests that use the PUT and POST methods will add an Expect: 100-continue header to the request and it will expect to receive a 100-Continue response from the server to indicate that the client should send the data to be posted. This mechanism allows clients to avoid sending large amounts of data over the network when the server, based on the request headers, intends to reject the request.

However, once the entire payload is received on the server end, other errors may still occur. And if Windows Azure clients have tested the client well enough to ensure that it is not sending any bad requests, clients could turn off 100-continue so that the entire request is sent in one roundtrip. This is especially true when clients send small size storage objects.

```csharp
ServicePointManager.Expect100Continue = false;
```

### Pattern/Recursive in DMLib
The following matrix explains how the DirectoryOptions.Recursive and DirectoryOptions.SearchPattern properties work in DMLib.

<table>
  <tr>
    <th>Source</th>
    <th>Search Pattern</th>
    <th>Recursive</th>
    <th>Search Pattern Example</th>
    <th>Comments</th>
  </tr>
  <tr>
    <td>Local</td>
    <td>Wildcard Match</td>
    <td>TRUE</td>
    <td>"foo*.png"</td>
    <td>The search pattern is a standard wild card match that is applied to the current directory and all subdirectories.</td>
  </tr>
  <tr>
    <td>Local</td>
    <td>Wildcard Match</td>
    <td>FALSE</td>
    <td>"foo*.png"</td>
    <td>The search pattern is a standard wild card match that is applied to the current directory only.</td>
  </tr>
  <tr>
    <td>Azure Blob</td>
    <td>Prefix Match</td>
    <td>TRUE</td>
    <td>&lt;domainname&gt;/&lt;container&gt;/&lt;virtualdirectory&gt;/&lt;blobprefix&gt;<br><br>"blah.blob.core.windows.net/ipsum/lorem/foo*"</td>
    <td>The search pattern is a prefix match.</td>
  <tr>
    <td>Azure Blob</td>
    <td>Exact Match</td>
    <td>FALSE</td>
    <td>&lt;domainname&gt;/&lt;container&gt;/&lt;virtualdirectory&gt;/&lt;fullblobname&gt;<br><br>"blah.blob.core.windows.net/ipsum/lorem/foobar.png"</td>
    <td>The search pattern is an exact match. If the search pattern is an empty string, no blobs will be matched.</td>
  <tr>
    <td>Azure File</td>
    <td>N/A</td>
    <td>TRUE</td>
    <td>N/A</td>
    <td>Recursive search is not supported and will return an error.</td>
  </tr>
  <tr>
    <td>Azure File</td>
    <td>Exact Match</td>
    <td>FALSE</td>
    <td>&lt;domainname&gt;/&lt;share&gt;/&lt;directory&gt;/&lt;fullfilename&gt;<br><br>"blah.files.core.windows.net/ipsum/lorem/foobar.png"</td>
    <td>The search pattern is an exact match. If the search pattern is an empty string, no files will be matched.</td>
  </tr>
</table>

- Default pattern option:
  - Local:*
  - Blob: Empty string
  - File: Empty string

- Default recursive option: false

### Choose Copy Method
DMLib supports three copy methods:
- Synchronous Copy  
  DMLib downloads data from source to memory, and uploads the data from memory to destination.
    
- Service Side Asynchronous Copy  
  DMLib sends request to Azure Storage Server to start the copying, and monitors its status until the copying is completed.
      
- Service Side Synchronous Copy  
  DMLib leverages [Put Block From URL](https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-from-url), [Append Block From URL](https://docs.microsoft.com/en-us/rest/api/storageservices/append-block-from-url), [Put Page From URL](https://docs.microsoft.com/en-us/rest/api/storageservices/put-page-from-url) to copy Azure Storage Blobs.
 
Following is suggested copy method for different scenarios:
- From performance aspect: <br> 
Based on Azure Storage Server's [SLA for Storage Accounts](https://azure.microsoft.com/en-us/support/legal/sla/storage/v1_5/), following are suggestions when copying performance is most important to you: 
  - To copy between blobs or files inner storage account, Service Side Asynchronous Copy would be suggested.
  - To copy Blobs, Service Side Synchronous Copy would be suggested.
  - To copy Files or copy between Blobs and Files, Synchronous Copy would be suggested. To achieve best copying performance, Synchronous Copy would need a powerful machine in Azure.
- From cost aspect, Service Side Asynchronous Copy would cost least. 
- From data flow approach, Synchronous Copy would be well controlled. With Synchronous Copy, the data will go through the network configured by you.
- From supported directions, Synchronous Copy and Service Side Asynchronous Copy support more directions than Service Side Synchronous Copy. See details in below table
  
Following table shows supported directions with different copy method.
  <table>
  <tr>
    <td></td>
    <th scope="col">Append Blob</th>
    <th scope="col">Block Blob</th>
    <th scope="col">Page Blob</th>
    <th scope="col">Azure File</th>
  </tr>
  <tr>
    <th scope="row">Append Blob</th>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy <br> Service Side Synchronous Copy</td>
    <td>N/A</td>
    <td>N/A</td>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy</td>
  </tr>
  <tr>
    <th scope="row">Block Blob</th>
    <td>N/A</td>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy <br> Service Side Synchronous Copy</td>
    <td>N/A</td>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy</td>
  </tr>
  <tr>
    <th scope="row">Page Blob</th>
    <td>N/A</td>
    <td>N/A</td>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy <br> Service Side Synchronous Copy</td>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy</td>
  </tr>
  <tr>
    <th scope="row">File</th>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy</td>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy</td>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy</td>
    <td>Synchronous Copy <br> Service Side Asynchronous Copy</td>
  </tr>
</table>

# Need Help?
Be sure to check out the Microsoft Azure [Developer Forums on MSDN](http://go.microsoft.com/fwlink/?LinkId=234489) if you have trouble with the provided code or use StackOverflow.


# Collaborate & Contribute

We gladly accept community contributions.

- Issues: Please report bugs using the Issues section of GitHub
- Forums: Interact with the development teams on StackOverflow or the Microsoft Azure Forums
- Source Code Contributions: Please follow the [contribution guidelines for Microsoft Azure open source](http://azure.github.io/guidelines.html) that details information on onboarding as a contributor

For general suggestions about Microsoft Azure please use our [UserVoice forum](http://feedback.azure.com/forums/34192--general-feedback).


# Learn More

- [Storage Data Movement Library API reference](https://azure.github.io/azure-storage-net-data-movement)
- [Storage Client Library Reference for .NET - MSDN](https://docs.microsoft.com/en-us/dotnet/api/overview/azure/storage?view=azure-dotnet)
- [Azure Storage Team Blog](http://blogs.msdn.com/b/windowsazurestorage/)
