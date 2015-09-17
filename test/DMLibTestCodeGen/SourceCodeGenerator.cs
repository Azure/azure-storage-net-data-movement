//------------------------------------------------------------------------------
// <copyright file="SourceCodeGenerator.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.CodeDom;
    using System.CodeDom.Compiler;
    using System.IO;
    using Microsoft.CSharp;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    internal static class GodeGeneratorConst
    {
        public const string RootNameSpace = "DMLibTest.Generated";
        public const string ClassInitMethodName = "GeneratedClassInit";
        public const string ClassCleanupMethodName = "GeneratedClassCleanup";
        public const string TestInitMethodName = "GeneratedTestInit";
        public const string TestCleanupMethodName = "GeneratedTestCleanup";
    }

    internal class SourceCodeGenerator
    {
        private const string SourceFileExtention = ".cs";
        private const string GeneratedSuffix = "_Generated";

        private string outputPath;

        public SourceCodeGenerator(string outputPath)
        {
            this.outputPath = outputPath;
        }

        public void GenerateSourceCode(MultiDirectionTestClass testClass)
        {
            string sourceFileName = this.GetSourceFileName(testClass);

            if (testClass.MultiDirectionMethods.Count == 0)
            {
                Console.WriteLine("{0} has no multiple direction test case. Skip code generating...", testClass.ClassType.Name);
                return;
            }

            Console.WriteLine("Generating code for {0}", testClass.ClassType.Name);

            CodeCompileUnit compileUnit = new CodeCompileUnit();
            CodeNamespace rootNameSpace = new CodeNamespace(GodeGeneratorConst.RootNameSpace);

            this.AddImport(rootNameSpace);

            rootNameSpace.Types.Add(GetGeneratedClass(testClass));

            compileUnit.Namespaces.Add(rootNameSpace);

            this.WriteCodeToFile(compileUnit, Path.Combine(this.outputPath, sourceFileName));
        }

        private void AddImport(CodeNamespace nameSpace)
        {
            nameSpace.Imports.Add(new CodeNamespaceImport("DMLibTestCodeGen"));
            nameSpace.Imports.Add(new CodeNamespaceImport("Microsoft.VisualStudio.TestTools.UnitTesting"));
            nameSpace.Imports.Add(new CodeNamespaceImport("MS.Test.Common.MsTestLib"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System"));
        }

        private CodeTypeDeclaration GetGeneratedClass(MultiDirectionTestClass testClass)
        {
            CodeTypeDeclaration result = new CodeTypeDeclaration(this.GetGeneratedClassName(testClass));
            result.Attributes = MemberAttributes.Public;
            result.BaseTypes.Add(testClass.ClassType);

            CodeAttributeDeclaration testClassAttribute = new CodeAttributeDeclaration(
                new CodeTypeReference(typeof(TestClassAttribute)));

            result.CustomAttributes.Add(testClassAttribute);

            // Add initialize and cleanup method
            result.Members.Add(this.GetInitCleanupMethod(typeof(ClassInitializeAttribute), testClass));
            result.Members.Add(this.GetInitCleanupMethod(typeof(ClassCleanupAttribute), testClass));

            // No need to generate TestInitialize and TestCleanup Method.
            // Generated class can inherit from base class.

            // Expand multiple direction test case
            foreach (MultiDirectionTestMethod testMethod in testClass.MultiDirectionMethods)
            {
                this.AddGeneratedMethod(result, testMethod);
            }

            return result;
        }

        private CodeMemberMethod GetInitCleanupMethod(Type methodAttributeType, MultiDirectionTestClass testClass)
        {
            bool isStatic = false;
            string generatedMetholdName = string.Empty;
            string methodToInvokeName = string.Empty;
            CodeParameterDeclarationExpression parameterDec = null;

            if (methodAttributeType == typeof(ClassInitializeAttribute))
            {
                isStatic = true;
                generatedMetholdName = GodeGeneratorConst.ClassInitMethodName;
                methodToInvokeName = testClass.ClassInit.Name;
                parameterDec = new CodeParameterDeclarationExpression(typeof(TestContext), "testContext");
            }
            else if (methodAttributeType == typeof(ClassCleanupAttribute))
            {
                isStatic = true;
                generatedMetholdName = GodeGeneratorConst.ClassCleanupMethodName;
                methodToInvokeName = testClass.ClassCleanup.Name;
            }
            else
            {
                throw new ArgumentException("methodAttributeType");
            }

            CodeMemberMethod result = new CodeMemberMethod();
            result.Name = generatedMetholdName;

            // Add parameter list if needed
            if (parameterDec != null)
            {
                result.Parameters.Add(parameterDec);
            }

            CodeExpression callBase = null;
            if (isStatic)
            {
                result.Attributes = MemberAttributes.Public | MemberAttributes.Static;
                callBase = new CodeTypeReferenceExpression(testClass.ClassType.FullName);
            }
            else
            {
                result.Attributes = MemberAttributes.Public | MemberAttributes.Final;
                callBase = new CodeBaseReferenceExpression();
            }

            // Add methold attribute
            CodeAttributeDeclaration methodAttribute = new CodeAttributeDeclaration(
                new CodeTypeReference(methodAttributeType));
            result.CustomAttributes.Add(methodAttribute);

            // Add invoke statement
            CodeMethodInvokeExpression invokeExp = null;
            if (parameterDec != null)
            {
                CodeVariableReferenceExpression sourceParameter = new CodeVariableReferenceExpression(parameterDec.Name);
                invokeExp = new CodeMethodInvokeExpression(callBase, methodToInvokeName, sourceParameter);
            }
            else
            {
                invokeExp = new CodeMethodInvokeExpression(callBase, methodToInvokeName);
            }

            result.Statements.Add(invokeExp);

            return result;
        }

        private void AddGeneratedMethod(CodeTypeDeclaration generatedClass, MultiDirectionTestMethod testMethod)
        {
            foreach (var transferDirection in testMethod.GetTransferDirections())
            {
                string generatedMethodName = this.GetGeneratedMethodName(testMethod, transferDirection);

                CodeMemberMethod generatedMethod = new CodeMemberMethod();
                generatedMethod.Name = generatedMethodName;
                generatedMethod.Attributes = MemberAttributes.Public | MemberAttributes.Final;

                // Add TestCategoryAttribute to the generated method
                this.AddTestCategoryAttributes(generatedMethod, testMethod);
                this.AddTestCategoryAttribute(generatedMethod, MultiDirectionTag.MultiDirection);
                foreach (var tag in transferDirection.GetTags())
                {
                    this.AddTestCategoryAttribute(generatedMethod, tag);
                }

                CodeAttributeDeclaration testMethodAttribute = new CodeAttributeDeclaration(
                    new CodeTypeReference(typeof(TestMethodAttribute)));

                generatedMethod.CustomAttributes.Add(testMethodAttribute);

                foreach (var statement in transferDirection.EnumerateUpdateContextStatements())
                {
                    generatedMethod.Statements.Add(statement);
                }

                CodeMethodReferenceExpression callee = new CodeMethodReferenceExpression(
                    new CodeBaseReferenceExpression(), testMethod.MethodInfoObj.Name);
                CodeMethodInvokeExpression invokeExp = new CodeMethodInvokeExpression(callee);
                generatedMethod.Statements.Add(invokeExp);
                generatedClass.Members.Add(generatedMethod);
            }
        }

        private void AddTestCategoryAttributes(CodeMemberMethod method, MultiDirectionTestMethod testMethod)
        {
            foreach (var customAttribute in testMethod.MethodInfoObj.CustomAttributes)
            {
                if (customAttribute.AttributeType == typeof(TestCategoryAttribute))
                {
                    if (customAttribute.ConstructorArguments.Count != 1)
                    {
                        // Unrecognized attribute, skip
                        continue;
                    }

                    this.AddTestCategoryAttribute(
                        method,
                        new CodeSnippetExpression(customAttribute.ConstructorArguments[0].ToString()));
                }
            }
        }

        private void AddTestCategoryAttribute(CodeMemberMethod method, string tagName)
        {
            this.AddTestCategoryAttribute(method, new CodePrimitiveExpression(tagName));
        }

        private void AddTestCategoryAttribute(CodeMemberMethod method, CodeExpression expression)
        {
            CodeAttributeArgument testCategoryTag = new CodeAttributeArgument(expression);

            CodeAttributeDeclaration testCategoryAttribute = new CodeAttributeDeclaration(
                new CodeTypeReference(typeof(TestCategoryAttribute)),
                testCategoryTag);

            method.CustomAttributes.Add(testCategoryAttribute);
        }

        private string GetSourceFileName(MultiDirectionTestClass testClass)
        {
            return this.GetGeneratedClassName(testClass) + SourceFileExtention;
        }

        private string GetGeneratedClassName(MultiDirectionTestClass testClass)
        {
            return testClass.ClassType.Name + GeneratedSuffix;
        }

        private string GetGeneratedMethodName(MultiDirectionTestMethod testMethod, TestMethodDirection transferDirection)
        {
            // [MethodName]_[DirectionSuffix]
            return String.Format("{0}_{1}", testMethod.MethodInfoObj.Name, transferDirection.GetTestMethodNameSuffix());
        }

        private void WriteCodeToFile(CodeCompileUnit compileUnit, string sourceFileName)
        {
            CSharpCodeProvider provider = new CSharpCodeProvider();

            using (StreamWriter sw = new StreamWriter(sourceFileName, false))
            {
                using (IndentedTextWriter tw = new IndentedTextWriter(sw, "    "))
                {
                    provider.GenerateCodeFromCompileUnit(compileUnit, tw, new CodeGeneratorOptions());
                }
            }
        }
    }
}
