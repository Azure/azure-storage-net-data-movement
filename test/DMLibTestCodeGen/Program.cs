//------------------------------------------------------------------------------
// <copyright file="Program.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Reflection;
    using System.Threading;

    public enum FrameworkType
    { 
        DNet,
        DNetCore
    };

    public class Program
    {
        public static FrameworkType FrameWorkType = FrameworkType.DNet;

        public static void Main(string[] args)
        {
            if (args == null || (args.Length != 2 && args.Length != 3))
            {
                PrintHelp();
                return;
            }

            string dllName = args[0];
            string sourceFolder = args[1];

            if (args.Length == 3)
            {
                FrameWorkType = (FrameworkType)Enum.Parse(typeof(FrameworkType), args[2]);
            }

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
            Console.WriteLine("Usage: DMLibTestCodeGen.exe [InputDll] [OutputSourceFolder] [FramworkType]");
        }
    }
}
