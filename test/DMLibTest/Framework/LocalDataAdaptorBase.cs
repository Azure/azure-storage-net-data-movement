//------------------------------------------------------------------------------
// <copyright file="LocalDataAdaptorBase.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.IO;
    
    public abstract class LocalDataAdaptorBase<TDataInfo> : DataAdaptor<TDataInfo> where TDataInfo : IDataInfo
    {
        protected string BasePath
        {
            get;
            private set;
        }

        public override string StorageKey
        {
            get
            {
                throw new NotSupportedException("StorageKey is not supported in LocalDataAdaptorBase.");
            }
        }

        public LocalDataAdaptorBase(string basePath, SourceOrDest sourceOrDest)
        {
            // The folder pointed by basePath will be deleted when cleanup.
            this.BasePath = basePath;
            this.SourceOrDest = sourceOrDest;
        }

        public override string GetAddress(params string[] list)
        {
            string address = Path.Combine(this.BasePath, Path.Combine(list));
            return address + Path.DirectorySeparatorChar;
        }

        public override string GetSecondaryAddress(params string[] list)
        {
            throw new NotSupportedException("GetSecondaryAddress is not supported in LocalDataAdaptorBase.");
        }

        public override void CreateIfNotExists()
        {
            if (!Directory.Exists(this.BasePath))
            {
                Directory.CreateDirectory(this.BasePath);
            }
        }

        public override bool Exists()
        {
            return Directory.Exists(this.BasePath);
        }

        public override void WaitForGEO()
        {
            throw new NotSupportedException("WaitForGEO is not supported in LocalDataAdaptorBase.");
        }

        public override void Cleanup()
        {
            Helper.CleanupFolder(this.BasePath);
        }

        public override void DeleteLocation()
        {
            Helper.DeleteFolder(this.BasePath);
        }

        public override void MakePublic()
        {
            throw new NotSupportedException("MakePublic is not supported in LocalDataAdaptorBase.");
        }

        public override void Reset()
        {
            // Nothing to reset
        }

        public override string GenerateSAS(SharedAccessPermissions sap, int validatePeriod, string policySignedIdentifier = null)
        {
            throw new NotSupportedException("GenerateSAS is not supported in LocalDataAdaptorBase.");
        }

        public override void RevokeSAS()
        {
            throw new NotSupportedException("RevokeSAS is not supported in LocalDataAdaptorBase.");
        }
    }
}
