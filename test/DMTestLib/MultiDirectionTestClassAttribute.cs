//------------------------------------------------------------------------------
// <copyright file="MultiDirectionTestClassAttribute.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;

    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public class MultiDirectionTestClassAttribute : Attribute
    {
        public MultiDirectionTestClassAttribute()
        {
        }
    }
}
