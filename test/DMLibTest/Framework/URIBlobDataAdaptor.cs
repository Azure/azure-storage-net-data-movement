//------------------------------------------------------------------------------
// <copyright file="URIBlobDataAdaptor.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using DMLibTestCodeGen;
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

        public override object GetTransferObject(FileNode fileNode)
        {
            return base.GetCloudBlobReference(fileNode).Uri;
        }

        public override object GetTransferObject(DirNode dirNode)
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
