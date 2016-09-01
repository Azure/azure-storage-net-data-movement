//------------------------------------------------------------------------------
// <copyright file="DirectionFilter.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    internal abstract class DirectionFilter
    {
        private IDictionary<string, Func<string, object>> valueGenerators = new Dictionary<string, Func<string, object>>();

        protected void SetProperties(string queryString)
        {
            if (string.IsNullOrEmpty(queryString))
            {
                return;
            }

            this.AddValueGenerators();

            string[] keyValuePairs = queryString.Split(new char[] { ',' }, StringSplitOptions.None);

            foreach (var keyValuePair in keyValuePairs)
            {
                string key;
                string value;
                if (this.TryParseKeyValuePair(keyValuePair, out key, out value))
                {
                    var valueGen = this.valueGenerators[key];
                    object valueObject = valueGen(value);

                    PropertyInfo prop = this.GetType().GetProperty(key);
                    prop.SetValue(this, valueObject);
                }
                else
                {
                    throw new ArgumentException(string.Format("Invalid queryString: {0}", queryString), "queryString");
                }
            }
        }

        private bool TryParseKeyValuePair(string keyValuePair, out string key, out string value)
        {
            string[] keyValueArray = keyValuePair.Split(new char[] { '=' }, StringSplitOptions.None);
            if (keyValueArray.Length != 2)
            {
                key = null;
                value = null;
                return false;
            }

            key = keyValueArray[0].Trim();
            value = keyValueArray[1].Trim();
            return true;
        }

        protected virtual void AddValueGenerators()
        {
        }

        protected void AddValueGenerator(string propertyName, Func<string, object> valueGenerator)
        {
            this.valueGenerators.Add(propertyName, valueGenerator);
        }

        public abstract bool Filter(TestMethodDirection direction);
    }
}
