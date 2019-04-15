//------------------------------------------------------------------------------
// <copyright file="URIBlobDataAdaptor.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using DMLibTestCodeGen;
    using Microsoft.Azure.Storage.Auth;
    using System;
    using BlobTypeConst = DMLibTest.BlobType;
    internal class URIBlobDataAdaptor : CloudBlobDataAdaptor
    {
        public URIBlobDataAdaptor(TestAccount testAccount, string containerName)
            : base (testAccount, containerName, BlobTypeConst.Block, SourceOrDest.Source)
        {
            base.MakePublic();
        }

        public override void Reset()
        {
            // Do nothing, keep the container public
        }

        public override object GetTransferObject(string rootPath, FileNode fileNode, StorageCredentials credentials = null)
        {
            Uri result=  base.GetCloudBlobReference(rootPath, fileNode).SnapshotQualifiedUri;

            if (credentials != null)
            {
                result = credentials.TransformUri(result);
            }

            return result;
        }

        public override void ValidateMD5ByDownloading(object file)
        {
            throw new NotSupportedException();
        }

        public override object GetTransferObject(string rootPath, DirNode dirNode, StorageCredentials credentials = null)
        {
            throw new InvalidOperationException("Can't get directory transfer object in URI data adaptor.");
        }

        protected override string BlobType
        {
            get
            {
                DMLibDataType destDataType = DMLibTestContext.DestType;
                if (destDataType == DMLibDataType.PageBlob)
                {
                    return BlobTypeConst.Page;
                }
                else if (destDataType == DMLibDataType.AppendBlob)
                {
                    return BlobTypeConst.Append;
                }
                else
                {
                    return BlobTypeConst.Block;
                }
            }
        }
    }
}
