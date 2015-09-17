//------------------------------------------------------------------------------
// <copyright file="ITestDirection.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    public interface ITestDirection<TDataType> where TDataType : struct
    {
        TDataType SourceType
        {
            get;
        }

        TDataType DestType
        {
            get;
        }
    }
}
