//------------------------------------------------------------------------------
// <copyright file="BlobDataAdaptorBase.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Text;
    using Microsoft.Azure.Storage.Blob;
    using BlobTypeConst = DMLibTest.BlobType;

    public abstract class BlobDataAdaptorBase<TDataInfo> : DataAdaptor<TDataInfo> where TDataInfo : IDataInfo
    {
        private string delimiter;
        private readonly string defaultContainerName;
        private string containerName;

        public override string StorageKey
        {
            get
            {
                return this.BlobHelper.Account.Credentials.ExportBase64EncodedKey();
            }
        }

        public string ContainerName
        {
            get
            {
                return this.containerName;
            }

            set
            {
                this.containerName = value;
            }
        }

        public CloudBlobHelper BlobHelper
        {
            get;
            private set;
        }

        protected TestAccount TestAccount
        {
            get;
            private set;
        }

        protected string TempFolder
        {
            get;
            private set;
        }

        protected virtual string BlobType
        {
            get;
            private set;
        }

        public BlobDataAdaptorBase(TestAccount testAccount, string containerName, string blobType, SourceOrDest sourceOrDest, string delimiter = "/")
        {
            if (BlobTypeConst.Block != blobType && BlobTypeConst.Page != blobType && BlobTypeConst.Append != blobType)
            {
                throw new ArgumentException("blobType");
            }

            this.TestAccount = testAccount;
            this.BlobHelper = new CloudBlobHelper(testAccount.Account);
            this.delimiter = delimiter;
            this.containerName = containerName;
            this.defaultContainerName = containerName;
            this.TempFolder = Guid.NewGuid().ToString();
            this.BlobType = blobType;
            this.SourceOrDest = sourceOrDest;
        }

        public override string GetAddress(params string[] list)
        {
            return this.GetAddress(this.TestAccount.GetEndpointBaseUri(EndpointType.Blob), list);
        }

        public override string GetSecondaryAddress(params string[] list)
        {
            return this.GetAddress(this.TestAccount.GetEndpointBaseUri(EndpointType.Blob, true), list);
        }

        public override void CreateIfNotExists()
        {
            this.BlobHelper.CreateContainer(this.containerName);
        }

        public override bool Exists()
        {
            return this.BlobHelper.Exists(this.containerName);
        }

        private string GetAddress(string baseUri, params string[] list)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(baseUri + "/" + this.containerName + "/");

            foreach (string token in list)
            {
                if (!string.IsNullOrEmpty(token))
                {
                    builder.Append(token);
                    builder.Append(this.delimiter);
                }
            }

            return builder.ToString();
        }

        public override void WaitForGEO()
        {
            CloudBlobContainer container = this.BlobHelper.GetGRSContainer(this.containerName);
            Helper.WaitForTakingEffect(container.ServiceClient);
        }

        public override void Cleanup()
        {
            this.BlobHelper.CleanupContainer(this.containerName);
        }

        public override void DeleteLocation()
        {
            this.BlobHelper.DeleteContainer(this.containerName);
        }

        public override void Reset()
        {
            if (this.Exists())
            {
                this.BlobHelper.SetContainerAccessType(this.containerName, BlobContainerPublicAccessType.Off);
            }

            this.containerName = this.defaultContainerName;
        }

        public override string GenerateSAS(SharedAccessPermissions sap, int validatePeriod, string policySignedIdentifier = null)
        {
            if (null == policySignedIdentifier)
            {
                if (this.SourceOrDest == SourceOrDest.Dest)
                {
                    this.BlobHelper.CreateContainer(this.containerName);
                }

                return this.BlobHelper.GetSASofContainer(this.containerName, sap.ToBlobPermissions(), validatePeriod, false);
            }
            else
            {
                this.BlobHelper.CreateContainer(this.containerName);
                return this.BlobHelper.GetSASofContainer(this.containerName, sap.ToBlobPermissions(), validatePeriod, true, policySignedIdentifier);
            }
        }

        public override void RevokeSAS()
        {
            this.BlobHelper.ClearSASPolicyofContainer(this.containerName);
        }

        public override void MakePublic()
        {
            this.BlobHelper.SetContainerAccessType(this.containerName, BlobContainerPublicAccessType.Container);

            DMLibTestHelper.WaitForACLTakeEffect();
        }
    }
}
