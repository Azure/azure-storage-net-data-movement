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
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Microsoft.WindowsAzure.Storage.File;

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

        public bool IsServiceCopy
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

        public void CloseStreamIfNecessary()
        {
            Stream sourceStream = this.SourceObject as Stream;
            Stream destStream = this.DestObject as Stream;
            
            if (sourceStream != null)
            {
                sourceStream.Close();
            }

            if (destStream != null)
            {
                destStream.Close();
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
                IsServiceCopy = this.IsServiceCopy,
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

            return dataObject.ToString();
        }
    }
}
