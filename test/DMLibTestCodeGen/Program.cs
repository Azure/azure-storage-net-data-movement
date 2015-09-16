//------------------------------------------------------------------------------
// <copyright file="Program.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Reflection;

    public class Program
    {
        public static void Main(string[] args)
        {
            if (args == null || args.Length != 2)
            {
                PrintHelp();
                return;
            }

            string dllName = args[0];
            string sourceFolder = args[1];

            GenerateCode(dllName, sourceFolder);
        }

        private static void GenerateCode(string dllName, string outputFolder)
        {
            SourceCodeGenerator codeGen = new SourceCodeGenerator(outputFolder);

            Assembly assembly = Assembly.LoadFrom(dllName);

            foreach (Type type in assembly.GetTypes())
            {
                if (null != type.GetCustomAttribute(typeof(MultiDirectionTestClassAttribute)))
                {
                    MultiDirectionTestClass testClass = new MultiDirectionTestClass(type);
                    codeGen.GenerateSourceCode(testClass);
                }
            }
        }

        private static void PrintHelp()
        {
            Console.WriteLine("Usage: DMLibTestCodeGen.exe [InputDll] [OutputSourceFolder]");
        }
    }
}
