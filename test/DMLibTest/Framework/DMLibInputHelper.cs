//------------------------------------------------------------------------------
// <copyright file="DMLibInputHelper.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;

    public static class DMLibInputHelper
    {
        public static ShouldOverwriteCallback GetDefaultOverwiteCallbackY()
        {
            return (source, dest) =>
            {
                Test.Info("Overwrite true: {0} -> {1}", DMLibTestHelper.TransferInstanceToString(source), DMLibTestHelper.TransferInstanceToString(dest));
                return true;
            };
        }

        public static ShouldOverwriteCallback GetDefaultOverwiteCallbackN()
        {
            return (source, dest) =>
            {
                Test.Info("Overwrite false: {0} -> {1}", DMLibTestHelper.TransferInstanceToString(source), DMLibTestHelper.TransferInstanceToString(dest));
                return false;
            };
        }
    }
}
