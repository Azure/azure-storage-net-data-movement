//------------------------------------------------------------------------------
// <copyright file="TransferItem.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Threading;
    using DMLibTestCodeGen;
    using Microsoft.Azure.Storage.Blob;
    using Microsoft.Azure.Storage.DataMovement;
    using Microsoft.Azure.Storage.File;

    public class TransferItem
    {
        public object SourceObject
        {
            get;
            set;
        }

        public object DestObject
        {
            get;
            set;
        }

        public DMLibDataType SourceType
        {
            get;
            set;
        }

        public DMLibDataType DestType
        {
            get;
            set;
        }

        public CopyMethod CopyMethod
        {
            get;
            set;
        }

        public bool IsDirectoryTransfer
        {
            get;
            set;
        }

        public object Options
        {
            get;
            set;
        }

        public TransferContext TransferContext
        {
            get;
            set;
        }

        public CancellationToken CancellationToken
        {
            get;
            set;
        }

        public Action BeforeStarted
        {
            get;
            set;
        }

        public Action AfterStarted
        {
            get;
            set;
        }

        public TransferStatus FinalStatus
        {
            get;
            set;
        }

        public bool DisableStreamDispose
        {
            get;
            set;
        }

        public Exception Exception
        {
            get;
            set;
        }

        public void CloseStreamIfNecessary()
        {
            if (!DisableStreamDispose)
            {
                Stream sourceStream = this.SourceObject as Stream;
                Stream destStream = this.DestObject as Stream;

                if (sourceStream != null)
                {
#if DNXCORE50
                    sourceStream.Dispose();
#else
                    sourceStream.Close();
#endif
                }

                if (destStream != null)
                {
#if DNXCORE50
                    destStream.Dispose();
#else
                    destStream.Close();
#endif
                }
            }
        }

        public TransferItem Clone()
        {
            TransferItem newTransferItem = new TransferItem()
            {
                SourceObject = NewLocationObject(this.SourceObject),
                DestObject = NewLocationObject(this.DestObject),
                SourceType = this.SourceType,
                DestType = this.DestType,
                IsDirectoryTransfer = this.IsDirectoryTransfer,
                CopyMethod = this.CopyMethod,
                Options = this.Options,
            };

            return newTransferItem;
        }

        private static object NewLocationObject(object locationObject)
        {
            if (locationObject is CloudBlob)
            {
                CloudBlob cloudBlob = locationObject as CloudBlob;
                if (cloudBlob is CloudPageBlob)
                {
                    return new CloudPageBlob(cloudBlob.SnapshotQualifiedUri, cloudBlob.ServiceClient.Credentials);
                }
                else if (cloudBlob is CloudBlockBlob)
                {
                    return new CloudBlockBlob(cloudBlob.SnapshotQualifiedUri, cloudBlob.ServiceClient.Credentials);
                }
                else if (cloudBlob is CloudAppendBlob)
                {
                    return new CloudAppendBlob(cloudBlob.SnapshotQualifiedUri, cloudBlob.ServiceClient.Credentials);
                }
                else
                {
                    throw new ArgumentException(string.Format("Unsupported blob type: {0}", cloudBlob.BlobType), "locationObject");
                }
            }
            else if (locationObject is CloudFile)
            {
                CloudFile cloudFile = locationObject as CloudFile;
                CloudFile newCloudFile = new CloudFile(cloudFile.Uri, cloudFile.ServiceClient.Credentials);
                return newCloudFile;
            }
            else
            {
                return locationObject;
            }
        }

        public override string ToString()
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "{0} -> {1}",
                this.GetDataObjectString(this.SourceObject),
                this.GetDataObjectString(this.DestObject));
        }

        private string GetDataObjectString(object dataObject)
        {
            if (dataObject is CloudBlob)
            {
                return (dataObject as CloudBlob).SnapshotQualifiedUri.ToString();
            }
            else if (dataObject is CloudFile)
            {
                return (dataObject as CloudFile).Uri.ToString();
            }
            else if (dataObject is CloudBlobDirectory)
            {
                return (dataObject as CloudBlobDirectory).Uri.ToString();
            }
            else if (dataObject is CloudFileDirectory)
            {
                return (dataObject as CloudFileDirectory).Uri.ToString();
            }

            return dataObject.ToString();
        }
    }
}
