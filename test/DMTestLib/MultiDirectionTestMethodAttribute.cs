//------------------------------------------------------------------------------
// <copyright file="MultiDirectionTestMethodAttribute.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Collections.Generic;

    public abstract class MultiDirectionTestMethodAttribute : Attribute
    {
        protected string[] Tags
        {
            get;
            private set;
        }

        protected MultiDirectionTestMethodAttribute(string[] tags = null)
        {
            this.Tags = tags ?? new string[0];
        }

        internal abstract IEnumerable<TestMethodDirection> ExtractDirections();
    }
}
