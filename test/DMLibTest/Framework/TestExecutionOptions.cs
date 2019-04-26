//------------------------------------------------------------------------------
// <copyright file="TestExecutionOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using Microsoft.Azure.Storage.Auth;
    using System;

    public class TestExecutionOptions<TDataInfo> where TDataInfo : IDataInfo
    {
        public const int DefaultTimeoutInMs = 20 * 60 * 1000; // 20 min

        public TestExecutionOptions()
        {
            this.TimeoutInMs = DefaultTimeoutInMs;
            this.DestTransferDataInfo = default(TDataInfo);
            this.DisableSourceGenerator = false;
            this.DisableDestinationFetch = false;
            this.DisableSourceCleaner = false;
            this.LimitSpeed = false;
        }

        public int TimeoutInMs
        {
            get;
            set;
        }

        public TDataInfo DestTransferDataInfo
        {
            get;
            set;
        }

        public bool IsDirectoryTransfer
        {
            get;
            set;
        }

        public bool DisableSourceGenerator
        {
            get;
            set;
        }

        public bool DisableSourceCleaner
        {
            get;
            set;
        }

        public bool DisableDestinationFetch
        {
            get;
            set;
        }

        public bool LimitSpeed
        {
            get;
            set;
        }

        public int? BlockSize { get; set; }

        public StorageCredentials SourceCredentials
        {
            get;
            set;
        }

        public StorageCredentials DestCredentials
        {
            get;
            set;
        }


        public Action<FileNode, TransferItem> TransferItemModifier;
        
        public Action AfterDataPrepared;

        public Action AfterAllItemAdded;
    }
}
