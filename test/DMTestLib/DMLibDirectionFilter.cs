//------------------------------------------------------------------------------
// <copyright file="DMLibDirectionFilter.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;

    internal class DMLibDirectionFilter : DirectionFilter
    {
        public DMLibCopyMethod? CopyMethod
        {
            get;
            set;
        }

        public DMLibDataType SourceType
        {
            get;
            set;
        }

        public DMLibDataType DestType
        {
            get;
            set;
        }

        public DMLibDirectionFilter(string queryString = null)
        {
            this.CopyMethod = null;
            this.SourceType = DMLibDataType.Unspecified;
            this.DestType = DMLibDataType.Unspecified;

            this.SetProperties(queryString);
        }

        protected override void AddValueGenerators()
        {
            base.AddValueGenerators();

            this.AddValueGenerator("IsAsync", ParseNullableBoolean);
            this.AddValueGenerator("SourceType", ParseDMLibDataType);
            this.AddValueGenerator("DestType", ParseDMLibDataType);
        }

        private static object ParseNullableBoolean(string value)
        {
            return (bool?)Boolean.Parse(value);
        }

        private static object ParseDMLibDataType(string value)
        {
            return Enum.Parse(typeof(DMLibDataType), value, true);
        }

        public override bool Filter(TestMethodDirection direction)
        {
            DMLibTransferDirection DMLibDirection = direction as DMLibTransferDirection;
            
            if (DMLibDirection == null)
            {
                throw new ArgumentException("DMLibDirectionFilter is only applicable to DMLibTransferDirection.", "direction");
            }

            if (this.CopyMethod != null && this.CopyMethod != DMLibDirection.CopyMethod)
            {
                return false;
            }

            if (this.SourceType != DMLibDataType.Unspecified && !this.SourceType.HasFlag(DMLibDirection.SourceType))
            {
                return false;
            }

            if (this.DestType != DMLibDataType.Unspecified && !this.DestType.HasFlag(DMLibDirection.DestType))
            {
                return false;
            }

            return true;
        }
    }
}
