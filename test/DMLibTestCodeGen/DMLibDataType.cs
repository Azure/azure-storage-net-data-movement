//------------------------------------------------------------------------------
// <copyright file="DMLibDataType.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Collections.Generic;

    [Flags]
    public enum DMLibDataType : int
    {
        Unspecified = 0x0,
        Stream = 0x01,
        URI = 0x02,
        Local = 0x04,
        CloudFile = 0x08,
        BlockBlob = 0x10,
        PageBlob = 0x20,
        AppendBlob = 0x40,

        CloudBlob = PageBlob | BlockBlob | AppendBlob,
        Cloud = CloudBlob | CloudFile,
        All = Local | Cloud,
    }

    internal static class DMLibDataTypeExtentions
    {
        public static IEnumerable<DMLibDataType> Extract(this DMLibDataType type)
        {
            DMLibDataType[] dataTypesToExtract = 
            {
                DMLibDataType.Stream,
                DMLibDataType.URI,
                DMLibDataType.Local,
                DMLibDataType.CloudFile,
                DMLibDataType.BlockBlob,
                DMLibDataType.PageBlob,
                DMLibDataType.AppendBlob,
            };

            foreach (var dataType in dataTypesToExtract)
            {
                if (type.HasFlag(dataType))
                {
                    yield return dataType;
                }
            }
        }
    }
}
