//------------------------------------------------------------------------------
// <copyright file="TestConfig.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;

namespace MS.Test.Common.MsTestLib
{
    public class TestConfig
    {
        private string DefaultConfigFileName = "TestData.xml";

        public TestConfig(string configFile)
        {
            testParams = new Dictionary<string, string>();
            testClasses = new Dictionary<string, ClassConfig>();

            //Initialze: read default config file TestData.xml and then read configFile file
            if(string.IsNullOrEmpty(configFile))
            {
                configFile = "TestData.xml";
            }
            if (File.Exists(DefaultConfigFileName))
            {
                ReadConfig(DefaultConfigFileName);      //read default config file: TestData.xml
            }
            if (File.Exists(configFile))
            {
                ReadConfig(configFile);                 //read configFile file: e.g MyTestData.xml, configuration in this file will cover settings in TestData.xml
            }
            else
            {
                throw new FileNotFoundException(String.Format("{0} not found", configFile));
            }
        }
        private void ReadConfig(string configFile)
        {
            if (string.IsNullOrEmpty(configFile))
            {
                throw new ArgumentNullException();  //illegal use
            }
            XmlDocument config = new XmlDocument();
            try
            {
#if DOTNET5_4
                Stream configStream = File.OpenRead(configFile);
                config.Load(configStream);
#else
                config.Load(configFile);
#endif
            }
            catch (FileNotFoundException)
            {
                string errorMsg = string.Format("{0} file not found", configFile);
                throw new FileNotFoundException(errorMsg);
            }
            catch (Exception)
            {
                throw;
            }
#if DOTNET5_4
            System.Xml.XPath.XPathNavigator navigator = config.DocumentElement.CreateNavigator();
            XmlNode root = navigator.SelectSingleNode("/TestConfig").UnderlyingObject as XmlNode;
#else
            XmlNode root = config.SelectSingleNode("TestConfig");
#endif
            if (root != null)
            {
                foreach (XmlNode node in root.ChildNodes)
                {
                    XmlElement eleNode = node as XmlElement;
                    if (eleNode == null)
                    {
                        continue;
                    }

                    if (string.Compare(eleNode.Name.ToLower(), "testclass") == 0 && eleNode.Attributes["name"] != null)
                    {
                        ClassConfig classConfig = this[eleNode.Attributes["name"].Value];
                        if(classConfig == null)
                            classConfig = new ClassConfig();
                        foreach (XmlNode subnode in eleNode.ChildNodes)
                        {
                            XmlElement eleSubnode = subnode as XmlElement;
                            if (eleSubnode == null)
                            {
                                continue;
                            }

                            if (string.Compare(eleSubnode.Name.ToLower(), "testmethod") == 0 && eleSubnode.Attributes["name"] != null)
                            {
                                MethodConfig methodConfig = classConfig[eleSubnode.Attributes["name"].Value];
                                if (methodConfig == null)
                                    methodConfig = new MethodConfig();
                                foreach (XmlNode methodParamNode in eleSubnode.ChildNodes)
                                {
                                    XmlElement eleMethodParamNode = methodParamNode as XmlElement;
                                    if (eleMethodParamNode == null)
                                    {
                                        continue;
                                    }
                                    methodConfig[eleMethodParamNode.Name] = eleMethodParamNode.InnerText;

                                }
                                classConfig[eleSubnode.Attributes["name"].Value] = methodConfig;
                                continue;
                            }

                            classConfig.ClassParams[eleSubnode.Name] = eleSubnode.InnerText;

                        }
                        this[eleNode.Attributes["name"].Value] = classConfig;
                        continue;

                    }

                    TestParams[eleNode.Name] = eleNode.InnerText;

                }
            }
        }

        private Dictionary<string, string> testParams = null;

        public Dictionary<string, string> TestParams
        {
            get { return testParams; }
            set { testParams = value; }
        }

        private Dictionary<string, ClassConfig> testClasses;

        public ClassConfig this[string className]
        {
            get
            {
                if (testClasses.ContainsKey(className))
                {
                    return testClasses[className];
                }
                else
                {
                    return null;
                }
            }

            set
            {
                testClasses[className] = value;
            }
        }

        public string Get(string paramName)
        {
            //first search the method params
            if (this[Test.FullClassName] != null)
            {
                if (this[Test.FullClassName][Test.MethodName] != null)
                {
                    if (this[Test.FullClassName][Test.MethodName].MethodParams.ContainsKey(paramName))
                    {
                        return this[Test.FullClassName][Test.MethodName].MethodParams[paramName].Trim();
                    }
                }

                if (this[Test.FullClassName].ClassParams.ContainsKey(paramName))
                {
                    return this[Test.FullClassName].ClassParams[paramName].Trim();
                }
            }

            if (TestParams.ContainsKey(paramName))
            {
                return TestParams[paramName].Trim();
            }

            throw new ArgumentException("The test param does not exist.", paramName);

            //return null;
            
        }

    }

}
