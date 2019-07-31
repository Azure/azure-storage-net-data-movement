//------------------------------------------------------------------------------
// <copyright file="DMLibTestContext.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    public class DMLibTestContext : MultiDirectionTestContext<DMLibDataType>
    {
        public static DMLibCopyMethod CopyMethod
        {
            get;
            set;
        }
    }
}
