//------------------------------------------------------------------------------
// <copyright file="MultiDirectionTestHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using DMLibTestCodeGen;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.File;
    using MS.Test.Common.MsTestLib;

    public static class MultiDirectionTestHelper
    {
        public static void WaitUntilFileCreated(string rootPath, FileNode fileNode, DataAdaptor<DMLibDataInfo> dataAdaptor, DMLibDataType dataType, int timeoutInSec = 300)
        {
            Func<bool> checkFileCreated = null;

            if (dataType == DMLibDataType.Local)
            {
                string filePath = dataAdaptor.GetAddress() + fileNode.GetLocalRelativePath();
                checkFileCreated = () =>
                {
                    return File.Exists(filePath);
                };
            }
            else if (dataType == DMLibDataType.PageBlob ||
                     dataType == DMLibDataType.AppendBlob)
            {
                CloudBlobDataAdaptor blobAdaptor = dataAdaptor as CloudBlobDataAdaptor;

                checkFileCreated = () =>
                {
                    CloudBlob cloudBlob = blobAdaptor.GetCloudBlobReference(rootPath, fileNode);
                    return cloudBlob.Exists(options: HelperConst.DefaultBlobOptions);
                };
            }
            else if (dataType == DMLibDataType.BlockBlob)
            {
                CloudBlobDataAdaptor blobAdaptor = dataAdaptor as CloudBlobDataAdaptor;

                checkFileCreated = () =>
                {
                    CloudBlockBlob blockBlob = blobAdaptor.GetCloudBlobReference(rootPath, fileNode) as CloudBlockBlob;
                    try
                    {
                        return blockBlob.DownloadBlockList(BlockListingFilter.All, options: HelperConst.DefaultBlobOptions).Any();
                    }
#if EXPECT_INTERNAL_WRAPPEDSTORAGEEXCEPTION
                    catch (Exception e) when (e is StorageException || (e is AggregateException && e.InnerException is StorageException))
#else
                    catch (StorageException)
#endif
                    {
                        return false;
                    }
                };
            }
            else if (dataType == DMLibDataType.CloudFile)
            {
                CloudFileDataAdaptor fileAdaptor = dataAdaptor as CloudFileDataAdaptor;

                checkFileCreated = () =>
                {
                    CloudFile cloudFile = fileAdaptor.GetCloudFileReference(rootPath, fileNode);
                    return cloudFile.Exists(options: HelperConst.DefaultFileOptions);
                };
            }
            else
            {
                Test.Error("Unexpected data type: {0}", DMLibTestContext.SourceType);
            }

            MultiDirectionTestHelper.WaitUntil(checkFileCreated, timeoutInSec);
        }

        private static void WaitUntil(Func<bool> condition, int timeoutInSec)
        {
            DateTime nowTime = DateTime.Now;
            DateTime timeOut = nowTime.AddSeconds(timeoutInSec);
            while (timeOut > DateTime.Now)
            {
                if (condition())
                {
                    return;
                }

                Thread.Sleep(100);
            }

            Test.Error("WaitUntil: condition doesn't meet within timeout {0} second(s).", timeoutInSec);
        }

        public static void PrintTransferDataInfo(IDataInfo dataInfo)
        {
            if (null == dataInfo)
            {
                Test.Info("TransferDataInfo is null");
            }
            else
            {
                Test.Info(dataInfo.ToString());
            }
        }
    }
}
