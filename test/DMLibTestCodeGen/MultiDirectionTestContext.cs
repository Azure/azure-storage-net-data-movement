//------------------------------------------------------------------------------
// <copyright file="MultiDirectionTestContext.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    public class MultiDirectionTestContext<TDataType> where TDataType : struct
    {
        public static TDataType SourceType
        {
            get;
            set;
        }

        public static TDataType DestType
        {
            get;
            set;
        }
    }
}
