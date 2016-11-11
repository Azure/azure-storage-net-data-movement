//------------------------------------------------------------------------------
// <copyright file="TestExecutionOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using Microsoft.WindowsAzure.Storage.Auth;
    using System;

    public class TestExecutionOptions<TDataInfo> where TDataInfo : IDataInfo
    {
        public const int DefaultTimeoutInMs = 20 * 60 * 1000; // 20 min

        public TestExecutionOptions()
        {
            this.TimeoutInMs = DefaultTimeoutInMs;
            this.DestTransferDataInfo = default(TDataInfo);
            this.DisableDestinationFetch = false;
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
