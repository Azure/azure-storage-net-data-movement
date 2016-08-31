//------------------------------------------------------------------------------
// <copyright file="MultiDirectionTestMethodSetAttribute.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    public abstract class MultiDirectionTestMethodSetAttribute : MultiDirectionTestMethodAttribute
    {
        private List<MultiDirectionTestMethodAttribute> testMethodAttributes = new List<MultiDirectionTestMethodAttribute>();

        private List<DirectionFilter> directionFilters = new List<DirectionFilter>();

        protected void AddTestMethodAttribute(MultiDirectionTestMethodAttribute testMethodAttribute)
        {
            if (testMethodAttribute == null)
            {
                throw new ArgumentNullException("testMethodAttribute");
            }

            this.testMethodAttributes.Add(testMethodAttribute);
        }

        internal void AddDirectionFilter(DirectionFilter directionFilter)
        {
            this.directionFilters.Add(directionFilter);
        }

        internal override IEnumerable<TestMethodDirection> ExtractDirections()
        {
            foreach(var attribute in this.testMethodAttributes)
            {
                foreach(var direction in attribute.ExtractDirections())
                {
                    if (this.Filter(direction))
                    {
                        yield return direction;
                    }
                }
            }
        }

        private bool Filter(TestMethodDirection direction)
        {
            foreach(var directionFilter in this.directionFilters)
            {
                if (!directionFilter.Filter(direction))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
