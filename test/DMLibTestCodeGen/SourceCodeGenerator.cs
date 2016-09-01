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
    using MSUnittest = Microsoft.VisualStudio.TestTools.UnitTesting;

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

            if (FrameworkType.DNetCore == Program.FrameWorkType)
            {
                AddXunitLifecycleCode(testClass.ClassType.Name, result);
            }
            else if (FrameworkType.DNet == Program.FrameWorkType) // Add initialize and cleanup method for MSTest
            {
                CodeAttributeDeclaration testClassAttribute = new CodeAttributeDeclaration(
                            new CodeTypeReference(typeof(MSUnittest.TestClassAttribute)));

                result.CustomAttributes.Add(testClassAttribute);
                result.Members.Add(this.GetInitCleanupMethod(typeof(MSUnittest.ClassInitializeAttribute), testClass));
                result.Members.Add(this.GetInitCleanupMethod(typeof(MSUnittest.ClassCleanupAttribute), testClass));
            }

            // No need to generate TestInitialize and TestCleanup Method.
            // Generated class can inherit from base class.

            // Expand multiple direction test case
            foreach (MultiDirectionTestMethod testMethod in testClass.MultiDirectionMethods)
            {
                this.AddGeneratedMethod(result, testMethod);
            }

            return result;
        }

        private static void AddXunitLifecycleCode(string className, CodeTypeDeclaration classDeclaration)
        {
            // Add IClassFixture for class-level initialization and cleanup, and
            // CollectionAttribute to mimic MsTest's assembly-level intialization and cleanup.
            // (http://xunit.github.io/docs/shared-context.html)
            classDeclaration.BaseTypes.Add($"Xunit.IClassFixture<{className}Fixture>");
            var globalCollectionAttribute =
                new CodeAttributeDeclaration(
                    new CodeTypeReference(typeof(Xunit.CollectionAttribute)),
                    new CodeAttributeArgument(new CodeSnippetExpression("Collections.Global")));
            classDeclaration.CustomAttributes.Add(globalCollectionAttribute);

            // Add a logger field to hold a reference to XunitLogger, which will
            // redirect output from MsTestLib's Test class to xUnit's logger
            const string loggerFieldname = "logger";
            var loggerField = new CodeMemberField
            {
                Attributes = MemberAttributes.Private,
                Name = loggerFieldname,
                Type = new CodeTypeReference("XunitLogger")
            };
            classDeclaration.Members.Add(loggerField);
            var loggerFieldReference = new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), loggerFieldname);

            const string parameterName = "outputHelper";
            var constructor = new CodeConstructor();
            constructor.Parameters.Add(new CodeParameterDeclarationExpression(typeof(Xunit.Abstractions.ITestOutputHelper), parameterName));
            constructor.Statements.Add(new CodeAssignStatement(loggerFieldReference,
                new CodeObjectCreateExpression(new CodeTypeReference("XunitLogger"), new CodeSnippetExpression(parameterName))));
            constructor.Statements.Add(new CodeMethodInvokeExpression(
                new CodeMethodReferenceExpression(
                    new CodeTypeReferenceExpression("Test.Logger.Loggers"), "Add"), loggerFieldReference));
            constructor.Attributes = MemberAttributes.Public;
            classDeclaration.Members.Add(constructor);

            // Add a Dispose method to ensure the xUnit logger reference doesn't outlive the test class
            var dispose = new CodeMemberMethod
            {
                Attributes = MemberAttributes.Override | MemberAttributes.Family,
                Name = "Dispose",
                ReturnType = new CodeTypeReference(typeof(void)),
            };
            dispose.Parameters.Add(new CodeParameterDeclarationExpression(typeof(bool), "disposing"));
            dispose.Statements.Add(new CodeMethodInvokeExpression(
                new CodeBaseReferenceExpression(), "Dispose", new CodeSnippetExpression("true")));
            dispose.Statements.Add(new CodeMethodInvokeExpression(
                new CodeMethodReferenceExpression(
                    new CodeTypeReferenceExpression("Test.Logger.Loggers"), "Remove"), loggerFieldReference));
            classDeclaration.Members.Add(dispose);
        }

        private CodeMemberMethod GetInitCleanupMethod(Type methodAttributeType, MultiDirectionTestClass testClass)
        {
            bool isStatic = false;
            string generatedMetholdName = string.Empty;
            string methodToInvokeName = string.Empty;
            CodeParameterDeclarationExpression parameterDec = null;
            Type acctualAttributeType = methodAttributeType;

            if (methodAttributeType == typeof(MSUnittest.ClassInitializeAttribute))
            {
                isStatic = true;
                generatedMetholdName = GodeGeneratorConst.ClassInitMethodName;
                methodToInvokeName = testClass.ClassInit.Name;
                parameterDec = new CodeParameterDeclarationExpression(typeof(MSUnittest.TestContext), "testContext");
            }
            else if (methodAttributeType == typeof(MSUnittest.ClassCleanupAttribute))
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
                new CodeTypeReference(acctualAttributeType));
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

                CodeAttributeDeclaration testMethodAttribute = null;

                testMethodAttribute = new CodeAttributeDeclaration(
                    new CodeTypeReference(typeof(MSUnittest.TestMethodAttribute)));

                generatedMethod.CustomAttributes.Add(testMethodAttribute);

                if (Program.FrameWorkType == FrameworkType.DNetCore)
                {
                    // add TestStartEndAttribute to ensure Test.Start and Test.End will be called as expected
                    generatedMethod.CustomAttributes.Add(new CodeAttributeDeclaration("TestStartEnd"));
                }

                foreach (var statement in TransferDirectionExtensions.EnumerateUpdateContextStatements(transferDirection as DMLibTransferDirection))
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
                if (customAttribute.AttributeType == typeof(MSUnittest.TestCategoryAttribute))
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

            CodeAttributeDeclaration testCategoryAttribute = null;

            testCategoryAttribute = new CodeAttributeDeclaration(
                new CodeTypeReference(typeof(MSUnittest.TestCategoryAttribute)), testCategoryTag);

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
