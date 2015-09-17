//------------------------------------------------------------------------------
// <copyright file="MultiDirectionTestClass.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    internal class MultiDirectionTestClass
    {
        public Type ClassType
        {
            private set;
            get;
        }

        public MethodInfo TestInit
        {
            private set;
            get;
        }

        public MethodInfo TestCleanup
        {
            private set;
            get;
        }

        public MethodInfo ClassInit
        {
            private set;
            get;
        }

        public MethodInfo ClassCleanup
        {
            private set;
            get;
        }

        public List<MultiDirectionTestMethod> MultiDirectionMethods
        {
            private set;
            get;
        }

        public MultiDirectionTestClass(Type type)
        {
            this.ClassType = type;
            this.MultiDirectionMethods = new List<MultiDirectionTestMethod>();

            this.ParseTestMethods(type);
        }

        private void ParseTestMethods(Type type)
        {
            foreach (MethodInfo methodInfo in type.GetMethods())
            {
                this.ParseTestMethod(methodInfo);
            }
        }

        private void ParseTestMethod(MethodInfo methodInfo)
        {
            bool isMultiDirectionMethod = false;
            foreach (Attribute attribute in methodInfo.GetCustomAttributes(true))
            {
                if (attribute is ClassInitializeAttribute)
                {
                    this.ClassInit = methodInfo;
                }
                else if (attribute is ClassCleanupAttribute)
                {
                    this.ClassCleanup = methodInfo;
                }
                else if (attribute is TestInitializeAttribute)
                {
                    this.TestInit = methodInfo;
                }
                else if (attribute is TestCleanupAttribute)
                {
                    this.TestCleanup = methodInfo;
                }
                else if (attribute is MultiDirectionTestMethodAttribute)
                {
                    isMultiDirectionMethod = true;
                }
                else if (attribute is MultiDirectionTestMethodSetAttribute)
                {
                    isMultiDirectionMethod = true;
                }
            }

            if (isMultiDirectionMethod)
            {
                this.MultiDirectionMethods.Add(new MultiDirectionTestMethod(methodInfo));
            }
        }
    }
}
