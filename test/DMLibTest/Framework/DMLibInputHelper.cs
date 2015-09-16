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
        public static OverwriteCallback GetDefaultOverwiteCallbackY()
        {
            return (sourcePath, destPath) =>
            {
                Test.Info("Overwrite true: {0} -> {1}", sourcePath, destPath);
                return true;
            };
        }

        public static OverwriteCallback GetDefaultOverwiteCallbackN()
        {
            return (sourcePath, destPath) =>
            {
                Test.Info("Overwrite false: {0} -> {1}", sourcePath, destPath);
                return false;
            };
        }
    }
}
