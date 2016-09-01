//------------------------------------------------------------------------------
// <copyright file="MultiDirectionTestMethod.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    public class MultiDirectionTestMethod
    {
        private HashSet<TestMethodDirection> transferDirections;

        public MethodInfo MethodInfoObj
        {
            get;
            private set;
        }

        public MultiDirectionTestMethod(MethodInfo methodInfo)
        {
            this.MethodInfoObj = methodInfo;
            transferDirections = new HashSet<TestMethodDirection>();

            foreach (Attribute attribute in methodInfo.GetCustomAttributes(true))
            {
                MultiDirectionTestMethodAttribute multiDirectionAttr = attribute as MultiDirectionTestMethodAttribute;
                if (null != multiDirectionAttr)
                {
                    this.ParseMultiDirectionAttribute(multiDirectionAttr);
                }
            }
        }

        public IEnumerable<TestMethodDirection> GetTransferDirections()
        {
            return this.transferDirections;
        }

        private void ParseMultiDirectionAttribute(MultiDirectionTestMethodAttribute multiDirectionAttr)
        {
            foreach (var direction in multiDirectionAttr.ExtractDirections())
            {
                if (this.transferDirections.Contains(direction) && direction.Tags.Any())
                {
                    this.transferDirections.Remove(direction);
                }

                this.transferDirections.Add(direction);
            }
        }
    }
}
