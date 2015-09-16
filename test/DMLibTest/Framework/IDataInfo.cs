//------------------------------------------------------------------------------
// <copyright file="IDataInfo.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    public interface IDataInfo
    {
        string ToString();

        IDataInfo Clone();
    }
}
