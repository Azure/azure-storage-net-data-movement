//------------------------------------------------------------------------------
// <copyright file="DataAdaptor.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using Microsoft.Azure.Storage.Auth;

    public abstract class DataAdaptor<TDataInfo> where TDataInfo : IDataInfo
    {
        public abstract string StorageKey
        {
            get;
        }

        public SourceOrDest SourceOrDest
        {
            get;
            protected set;
        }

        public abstract string GetAddress(params string[] list);

        public abstract string GetSecondaryAddress(params string[] list);

        public abstract object GetTransferObject(string rootPath, FileNode fileNode, StorageCredentials credentials = null);

        public abstract object GetTransferObject(string rootPath, DirNode dirNode, StorageCredentials credentials = null);

        public abstract void CreateIfNotExists();

        public abstract bool Exists();

        public abstract void WaitForGEO();

        public abstract void ValidateMD5ByDownloading(object file);

        public void GenerateData(TDataInfo dataInfo)
        {
            this.GenerateDataImp(dataInfo);

            if (SourceOrDest.Source == this.SourceOrDest)
            {
                MultiDirectionTestInfo.GeneratedSourceDataInfos.Add(dataInfo == null ? dataInfo : dataInfo.Clone());
            }
            else
            {
                MultiDirectionTestInfo.GeneratedDestDataInfos.Add(dataInfo == null ? dataInfo : dataInfo.Clone());
            }
        }

        public abstract TDataInfo GetTransferDataInfo(string rootDir);

        public abstract void Cleanup();

        public abstract void DeleteLocation();

        public abstract string GenerateSAS(SharedAccessPermissions sap, int validatePeriod, string policySignedIdentifier = null);

        public abstract void RevokeSAS();

        public abstract void MakePublic();

        public abstract void Reset();

        protected abstract void GenerateDataImp(TDataInfo dataInfo);
    }
}
