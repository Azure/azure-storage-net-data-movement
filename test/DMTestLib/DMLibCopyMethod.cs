//------------------------------------------------------------------------------
// <copyright file="DMLibCopyMethod.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

using System;

namespace DMLibTestCodeGen
{
    [Flags]
    public enum DMLibCopyMethod : int
    {
        SyncCopy = 0x01,
        ServiceSideAsyncCopy = 0x02,
        ServiceSideSyncCopy = 0x04,
    }
}
