using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq; 

/// <summary>
/// This Microservice wrapper/proxy class is used to interface with the ILEusion microservice to make 
/// IBM i data interactions via a web based MicroService. 
/// 
/// This .Net assembly is based on .Net standard so should work with .Net 4.6.1 and above or 
/// with .Net Core Version 2.0 and above on any platform, including IBMi Mono .Net.
/// 
/// Info on ILEusion and associated projects.
/// 
/// ILEusion - IBM i accessible via HTTP or Db. (Based on ILEastic and noxDB) 
/// Github link: https://github.com/sitemule/ILEusion
/// Web site: https://ileusion.com/
///
/// ILEAstic - An embedded application server for ILE on IBM i.
/// Github link: https://github.com/sitemule/ILEastic
/// 
/// noxDB - Not only XML. SQL,JSON and XML made easy for IBM i.
/// Github link: https://github.com/sitemule/noxDB
///  
/// Return data is returned in a usable .Net DataTable format or you can process the raw JSON responses yourself.
/// Return data can also be converted to a List object. 
/// Return data can also be extracted as XML, CSV or JSON strings or output files.
///  
/// Requirement: An ILEusion web service instance must be up and running on your IBMi.
///
/// Note: For appropriate security you should configure your ILEusion instance for user Authentication. 
/// To enable HTTP authentication use the UseHttpCredentials parameter and set it to True on the 
/// SetHttpUserInfo method.
///  
/// You can always refer to the ILEusion site for more info on the microservice framework.
/// </summary>
/// <remarks></remarks>

namespace ILEusion
{

/// <summary>
/// ILEusion service wrapper/proxy class
/// </summary>
public class ILEusionService
{
    private string _LastError;
    private DataTable _dtQueryResults;
    private string _ServiceURL = "";
    private string _User = "";
    private string _Password = "";
    private string _LastHttpStatusResponse = "";
    private string _LastHttpResponseData = "";
    private string _LastJsonResponse = "";
    private int _HttpTimeout = 10000;
    private bool _UseHttpCredentials = false;
    private string _HttpUser = "";
    private string _HttpPassword = "";
    private bool _encodeAuthBase64 = true;
    private bool _allowInvalidSslCertificates = false;

        #region "ILEusionAccessMethods"

        /// <summary>
        ///  Set base URL to ILEastic/ILEusion microservice server.
        ///  Set this value one time each time the class is instantiated.
        ///  </summary>
        ///  <param name="sBaseUrl">Base URL to set for path to ILEastic/ILEusion server.</param>
        ///  <returns>True-Success, False-Fail</returns>
        ///  <remarks></remarks>
        public bool SetServiceURL(string sBaseUrl)
        {
            try
            {
                _LastError = "";
                _ServiceURL = sBaseUrl;
                return true;
            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        ///  Set HTTP timeout for HTTP requests
        ///  </summary>
        ///  <param name="iHttpTimeout">HTTP timeout in milliseconds</param>
        ///  <returns>True-Success, False-Fail</returns>
        ///  <remarks></remarks>
        public bool SetHttpTimeout(int iHttpTimeout)
    {
        try
        {
            _LastError = "";
            _HttpTimeout = iHttpTimeout;
            return true;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }

        /// <summary>
        ///  Set ALL base user info parameters for XMLCGI program calls in a single method call.
        ///  Set this value one time each time the class Is instantiated.
        ///  This is a convenience method to set all connection info in a single call.
        ///  </summary>
        ///  <param name="serviceUrl">Base URL to set for path to ILEusion web service.</param>
        ///  <param name="ibmiUser">IBM i User and HTTP Auth</param>
        ///  <param name="ibmiPass">IBM i Password and HTTP Auth</param>
        ///  <param name="useHttpCredentials">Use Apache HTTP authentication credentials</param>
        ///  <param name="httpAuthUser">Http Auth user. Only set if HTTP auth credentials are different than IBMi user info and web server auth enabled.</param>
        ///  <param name="httpAuthPass">Http Auth password. Only set if HTTP auth credentials are different than IBMi user info and web server auth enabled.</param>
        ///  <param name="encodeAuthBase64">Base64 encode user and password in Auth header. true=encode to base64, false=no encoding.Default=true</param>
        ///  <param name="allowInvalidSslCertificates">Optional Allow invallid certs. true=Yes, false=no. Default=false - certs must be valid.</param>
        ///  <returns>True-Success, False-Fail</returns>
        ///  <remarks></remarks>
        public bool SetUserInfo(string serviceUrl, string ibmiUser, string ibmiPass, bool useHttpCredentials, int httpTimeout = 10000, string httpAuthUser = "", string httpAuthPass = "",bool encodeAuthBase64=true, bool allowInvalidSslCertificates = false)
    {
        try
        {
            _LastError = "";

            if (SetServiceURL(serviceUrl) == false)
                throw new Exception("Error setting service URL");

            if (SetUserInfo(ibmiUser, ibmiPass, useHttpCredentials) == false)
                throw new Exception("Error setting user info");

            // Change HTTP auth user and password to be other than default IBMi credentials
            if (httpAuthPass.Trim() != "" && httpAuthUser.Trim() != "")
            {
                if (SetHttpUserInfo(httpAuthUser, httpAuthPass, useHttpCredentials))
                    throw new Exception("Error setting HTTP user info");
            }

            // Set encode base 64 auth header
            _encodeAuthBase64 = encodeAuthBase64;

           // Set allow invalid certificates
           _allowInvalidSslCertificates = allowInvalidSslCertificates;

                return true;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }

        /// <summary>
        ///  Set base user info for ILEastic/ILEusion program calls.
        ///  Set this value one time each time the class Is instantiated.
        ///  This sets the IBM i user login info And also sets the default
        ///  HTTP auth user credentials for the service if HTTP authentication 
        ///  is enabled on the web service.
        ///  </summary>
        ///  <param name="sUser">IBM i User</param>
        ///  <param name="sPassword">IBM i Password</param>
        ///  <param name="UseHttpCredentials">Use Apache HTTP authentication credentials.</param>
        ///  <param name="encodeAuthBase64">Base64 encode user and password in Auth header. true=encode to base64, false=no encoding. Default=true</param>
        ///  <param name="allowInvalidSslCertificates">Optional Allow invallid certs. true=Yes, false=no. Default=false - certs must be valid.</param>
        ///  <returns>True-Success, False-Fail</returns>
        ///  <remarks></remarks>
        public bool SetUserInfo(string sUser, string sPassword, bool UseHttpCredentials,bool encodeAuthBase64 = true, bool allowInvalidSslCertificates=false)
    {
        try
        {
            _LastError = "";
            
            // Set IBM i user info
            _User = sUser;
            _Password = sPassword;
            
            // Set IBM i apache authentication user info default'
            _HttpUser = sUser;
            _HttpPassword = sPassword;
            
            // Set use HTTP authentication flag
            _UseHttpCredentials = UseHttpCredentials;
            
            // Set encode base 64 auth header
            _encodeAuthBase64 = encodeAuthBase64;

            // Set allow invalid certificates
            _allowInvalidSslCertificates = allowInvalidSslCertificates;

            return true;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }
    /// <summary>
    ///  Set ILEastic/ILEusion authenticated user credential info for service calls.
    ///  Set this value one time each time the class Is instantiated.
    ///  Also make sure you call SetUserInfo first which sets the the IBM i user And 
    ///  the default HTTP user And password to the same User And Password as SetUserInfo. 
    ///  The IBM i user profile And password can be overridden with SetHttpUserInfo 
    ///  if the Apache authentication user Is Not the same as the IBM i user profile 
    ///  Or it uses an authorization list Or LDAP user for HTTP authentication
    ///  if HTTP auth user Is different than IBM i user.
    ///  </summary>
    ///  <param name="sUser">IBM i Apache HTTP Server Web Site Auth User</param>
    ///  <param name="sPassword">IBM i Apache HTTP Server Web Site Auth Password</param>
    ///  <returns>True-Success, False-Fail</returns>
    ///  <remarks></remarks>
    public bool SetHttpUserInfo(string sUser, string sPassword, bool UseHttpCredentials)
    {
        try
        {
            _LastError = "";
            _HttpUser = sUser;
            _HttpPassword = sPassword;
            // Set use Apache HTTP authentication flag
            _UseHttpCredentials = UseHttpCredentials;
            return true;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }

    /// <summary>
    ///  Returns last error message string
    ///  </summary>
    ///  <returns>Last error message string</returns>
    ///  <remarks></remarks>
    public string GetLastError()
    {
        try
        {
            return _LastError;
        }
        catch (Exception)
        {
            return "";
        }
    }
    /// <summary>
    ///  Returns JSON response message string from last ILEastic/ILEusion service call.
    ///  </summary>
    ///  <returns>Last JSON response message string</returns>
    ///  <remarks></remarks>
    public string GetLastJsonResponse()
    {
        try
        {
            return _LastJsonResponse;
        }
        catch (Exception)
        {
            return "";
        }
    }
        /// <summary>
        ///  Returns last HTTP status response status message
        ///  </summary>
        ///  <returns>Last HTTP response status message string</returns>
        ///  <remarks></remarks>
        public string GetLastHttpStatusResponse()
        {
            try
            {
                return _LastHttpStatusResponse;
            }
            catch (Exception)
            {
                return "";
            }
        }
        /// <summary>
        ///  Returns last HTTP response data
        ///  </summary>
        ///  <returns>Last HTTP response data</returns>
        ///  <remarks></remarks>
        public string GetLastHttpResponseData()
        {
            try
            {
                return _LastHttpResponseData;
            }
            catch (Exception)
            {
                return "";
            }
        }
        /// <summary>
        ///  This function gets the DataTable of data loaded from XML with LoadDataSetFromXMLFile and returns as a CSV string
        ///  </summary>
        ///  <param name="sFieldSepchar">Field delimiter/separator. Default = Comma</param>
        ///  <param name="sFieldDataDelimChar">Field data delimiter character. Default = double quotes.</param>
        ///  <returns>CSV string from DataTable</returns>
        public string GetQueryResultsDataTableToCsvString(string sFieldSepchar = ",", string sFieldDataDelimChar = "\"")
    {
        try
        {
            _LastError = "";

            //string sHeadings = "";
            //string sBody = "";
            StringBuilder sCsvData = new StringBuilder();

            // first write a line with the columns name
            string sep = "";
            System.Text.StringBuilder builder = new System.Text.StringBuilder();
            foreach (DataColumn col in _dtQueryResults.Columns)
            {
                builder.Append(sep).Append(col.ColumnName);
                sep = sFieldSepchar;
            }
            sCsvData.AppendLine(builder.ToString());

            // then write all the rows
            foreach (DataRow row in _dtQueryResults.Rows)
            {
                sep = "";
                builder = new System.Text.StringBuilder();

                foreach (DataColumn col in _dtQueryResults.Columns)
                {
                    builder.Append(sep);
                    builder.Append(sFieldDataDelimChar).Append(row[col.ColumnName]).Append(sFieldDataDelimChar);
                    sep = sFieldSepchar;
                }
                sCsvData.AppendLine(builder.ToString());
            }

            // Return CSV output
            return sCsvData.ToString();
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return "";
        }
    }
    /// <summary>
    ///  This function gets the DataTable of XML data loaded from the last query with LoadDataSetFromXMLFile and returns as a CSV file
    ///  </summary>
    ///  <param name="sOutputFile">Output CSV file</param>
    ///  <param name="sFieldSepchar">Field delimiter/separator. Default = Comma</param>
    ///  <param name="sFieldDataDelimChar">Field data delimiter character. Default = double quotes.</param>
    ///  <param name="replace">Replace output file True=Replace file,False=Do not replace</param>
    ///  <returns>True-CSV file written successfully, False-Failure writing CSV output file.</returns>
    public bool GetQueryResultsDataTableToCsvFile(string sOutputFile, string sFieldSepchar = ",", string sFieldDataDelimChar = "\"", bool replace = false)
    {
        string sCsvWork;

        try
        {
            _LastError = "";

            // Delete existing file if replacing
            if (File.Exists(sOutputFile))
            {
                if (replace)
                    File.Delete(sOutputFile);
                else
                    throw new Exception("Output file " + sOutputFile + " already exists and replace not selected.");
            }

            // Get data and output
            using (System.IO.StreamWriter writer = new System.IO.StreamWriter(sOutputFile))
            {

                // Get CSV string
                sCsvWork = GetQueryResultsDataTableToCsvString(sFieldSepchar, sFieldDataDelimChar);

                // Write out CSV data
                writer.Write(sCsvWork);

                // Flush final output and close
                writer.Flush();
                writer.Close();

                return true;
            }
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }

    /// <summary>
    ///  This function gets the DataTable of data loaded from XML with LoadDataSetFromXMLFile and returns as a XML string
    ///  </summary>
    ///  <param name="sTableName">Table name. Default = "Table1"</param>
    ///  <param name="bWriteSchema">Write XML schema in return data</param>
    ///  <returns>XML string from data table</returns>
    public string GetQueryResultsDataTableToXmlString(string sTableName = "Table1", bool bWriteSchema = false)
    {
        string sRtnXml = "";

        try
        {
            _LastError = "";

            // if table not set, default to Table1
            if (sTableName.Trim() == "")
                sTableName = "Table1";

            // Export results to XML
            if (_dtQueryResults == null == false)
            {
                StringBuilder SB = new StringBuilder();
                System.IO.StringWriter SW = new System.IO.StringWriter(SB);
                _dtQueryResults = GetQueryResultsDataTable();
                _dtQueryResults.TableName = sTableName;
                // Write XMl with or without schema info
                if (bWriteSchema)
                    _dtQueryResults.WriteXml(SW, System.Data.XmlWriteMode.WriteSchema);
                else
                    _dtQueryResults.WriteXml(SW);
                sRtnXml = SW.ToString();
                SW.Close();
                return sRtnXml;
            }
            else
                throw new Exception("No data available. Error: " + GetLastError());
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return "";
        }
    }
    /// <summary>
    ///  This function gets the DataTable of XML data loaded from the last query with LoadDataSetFromXMLFile and returns as a CSV file
    ///  </summary>
    ///  <param name="sOutputFile">Output CSV file</param>
    ///  <param name="sTableName">Table name. Default = "Table1"</param>
    ///  <param name="bWriteSchema">Write XML schema in return data</param>
    ///  <param name="replace">Replace output file True=Replace file,False=Do not replace</param>
    ///  <returns>True-XML file written successfully, False-Failure writing XML output file.</returns>
    public bool GetQueryResultsDataTableToXmlFile(string sOutputFile, string sTableName = "Table1", bool bWriteSchema = false, bool replace = false)
    {
        string sXmlWork;

        try
        {
            _LastError = "";

            // Delete existing file if replacing
            if (File.Exists(sOutputFile))
            {
                if (replace)
                    File.Delete(sOutputFile);
                else
                    throw new Exception("Output file " + sOutputFile + " already exists and replace not selected.");
            }

            // Get data and output 
            using (System.IO.StreamWriter writer = new System.IO.StreamWriter(sOutputFile))
            {

                // Get XML string
                sXmlWork = GetQueryResultsDataTableToXmlString(sTableName, bWriteSchema);

                // Write out CSV data
                writer.Write(sXmlWork);

                // Flush final output and close
                writer.Flush();
                writer.Close();

                return true;
            }
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }
    /// <summary>
    ///  This function gets the DataTable of data loaded from XML with LoadDataSetFromXMLFile and returns as a JSON string
    ///  </summary>
    ///  <returns>CSV string from DataTable</returns>
    public string GetQueryResultsDataTableToJsonString(bool debugInfo = false)
    {

        // TODO - Use Newtonsoft JSON to convert to JSON

        string sJsonData = "";
        JsonHelper oJsonHelper = new JsonHelper();

        try
        {
            _LastError = "";

            // If data table is blank, bail
            if (_dtQueryResults == null)
                throw new Exception("Data table is Nothing. No data available.");

            // Convert DataTable to JSON
            sJsonData = oJsonHelper.DataTableToJsonWithStringBuilder(_dtQueryResults, debugInfo);

            // Return JSON output
            return sJsonData.ToString();
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return "";
        }
    }
    /// <summary>
    ///  This function gets the DataTable of XML data loaded from the last query with LoadDataSetFromXMLFile and returns as a JSON file
    ///  </summary>
    ///  <param name="sOutputFile">Output JSON file</param>
    ///  <param name="replace">Replace output file True=Replace file,False=Do not replace</param>
    ///  <returns>True-JSON file written successfully, False-Failure writing JSON output file.</returns>
    public bool GetQueryResultsDataTableToJsonFile(string sOutputFile, bool replace = false)
    {
        string sJsonWork;

        try
        {
            _LastError = "";

            // Delete existing file if replacing
            if (File.Exists(sOutputFile))
            {
                if (replace)
                    File.Delete(sOutputFile);
                else
                    throw new Exception("Output file " + sOutputFile + " already exists and replace not selected.");
            }

            // Get data and output 
            using (System.IO.StreamWriter writer = new System.IO.StreamWriter(sOutputFile))
            {

                // Get JSON string
                sJsonWork = GetQueryResultsDataTableToJsonString();

                // Write out JSON data
                writer.Write(sJsonWork);

                // Flush final output and close
                writer.Flush();
                writer.Close();

                return true;
            }
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }
    /// <summary>
    ///  This function gets the DataTable containing records from the last query response data loaded from XML.
    ///  </summary>
    ///  <returns>Data table of data or nothing if no data set</returns>
    ///  <remarks></remarks>
    public DataTable GetQueryResultsDataTable()
    {
        try
        {
            _LastError = "";

            return _dtQueryResults;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return null;
        }
    }

    /// <summary>
    ///  This function runs an SQL INSERT, UPDATE, DELETE or other action query against the DB2 database with selected SQL statement.
    ///  </summary>
    ///  <param name="sSQL">SQL INSERT, UPDATE and DELETE. Select is not allowed </param>
    ///  <returns>True - Query service call succeeded, False - Query service call failed</returns>
    ///  <remarks>Note: Committment control is disabled via the commit='none' option so journaling is not used at the moment on any files you plan to modify via INSERT/UPDATE/DELETE</remarks>
    public bool ExecuteSqlNonQuery(string sSQL)
    {
            // Clear data set
            _dtQueryResults = null;

            try
            {
                _LastError = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build SQL query post for executenonquery
                string jsonPostData = @"{""mode"":2,""query"":""@@sqlquery""}";
                jsonPostData = jsonPostData.Replace("@@sqlquery", sSQL);

                // Execute POST request
                _LastJsonResponse = ExecuteHttpJsonPostRequest(_ServiceURL.Trim() + "/sql", jsonPostData, _HttpTimeout, _UseHttpCredentials, false, _encodeAuthBase64, _allowInvalidSslCertificates);

                // Check query success
                if (_LastJsonResponse.Contains("\"success\":false")) // Bad query but service active
                {
                    throw new Exception("SQL query failed.");
                }
                else if (_LastJsonResponse.StartsWith("ERROR")) // HTTP error
                {
                    throw new Exception("SQL query failed. Most likely an HTTP error occurred.");
                }
                else if (_LastJsonResponse.Trim() == "") // No date in JSON buffer
                {
                    throw new Exception("SQL query failed. Most likely no data returned.");
                }
                else if (_LastJsonResponse.Trim() != "") // Data returned, all good
                {
                    return true;
                }
                else // Misc catch all error
                {
                    throw new Exception("SQL query failed. Most likely no data returned.");
                }

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        ///  This function queries the DB2 database with selected SQL statement, returns the XML response
        ///  and then loads the internal DataTable object with the returned records.
        ///  The internal results DataTable can be accessed by and of the GetDataTable* methods.
        ///  </summary>
        ///  <param name="sSQL">SQL Select. INSERT, UPDATE and DELETE not allowed </param>
        ///  <param name="sQueryResultOutputFile">Optional PC output file for XML response data. Otherwise data set is created from memory.</param>
        ///  <returns>True- Query service call succeeded, False-Query service call failed</returns>
        ///  <remarks></remarks>
        public bool ExecuteSqlQuery(string sSQL,string dataTableName="Table1")
    {

            // Clear data set
            _dtQueryResults = null;

            try
            {
                _LastError = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build SQL query post
                string jsonPostData = @"{""query"":""@@sqlquery""}";
                jsonPostData = jsonPostData.Replace("@@sqlquery", sSQL);

                // Execute POST request
                _LastJsonResponse = ExecuteHttpJsonPostRequest(_ServiceURL.Trim() + "/sql", jsonPostData, _HttpTimeout, _UseHttpCredentials, false, _encodeAuthBase64, _allowInvalidSslCertificates);

                // Check query success
                if (_LastJsonResponse.Contains("\"success\":false"))
                {
                    throw new Exception("SQL query failed.");
                }
                else
                {
                    // Load datatable with data results
                    var table = JsonConvert.DeserializeObject<DataTable>(_LastJsonResponse);
                    table.TableName = dataTableName;
                    _dtQueryResults = table;
                    return true;
                }

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }

        }

        /// <summary>
        ///  This function queries the DB2 database with selected SQL statement and returns results to data table all in one step 
        ///  </summary>
        ///  <param name="sSQL">SQL Select. INSERT, UPDATE and DELETE not allowed </param>
        ///  <param name="sQueryResultOutputFile">Optional PC output file for XML response data. Otherwise data set is created from memory.</param>
        ///  <returns>DataTable with results of query or Nothing</returns>
        ///  <remarks></remarks>
        public DataTable ExecuteSqlQueryToDataTable(string sSQL, string dataTableName = "Table1")
    {

        bool rtnquery;

        try
        {
            _LastError = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // Run query and load internal results DataTable
            rtnquery = ExecuteSqlQuery(sSQL,dataTableName);

            // Return results DataTable
            if (rtnquery)
                return GetQueryResultsDataTable();
            else
                throw new Exception("Query failed. Error: " + GetLastError());
        }
        catch (Exception ex)
        {
            _LastJsonResponse = GetLastJsonResponse();
            _LastError = ex.Message;
            return null;
        }
    }

    /// <summary>
    ///  This function queries the DB2 database with selected SQL statement and returns results to generic list all in one step.
    ///  Column names can optionally be returned in first row of generic list.
    ///  </summary>
    ///  <param name="sSQL">SQL Select. INSERT, UPDATE and DELETE not allowed </param>
    ///  <param name="firstRowColumnNames">Optional - Return first row as column names. False=No column names, True=Return column names. Default=False</param>
    ///  <returns>Generic List object with results of query or Nothing on error</returns>
    ///  <remarks></remarks>
    public List<List<object>> ExecuteSqlQueryToList(string sSQL, bool firstRowColumnNames = false,string dataTableName="Table1")
    {
        DataTable dtTemp;

        try
        {
            _LastError = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // Run query to DataTable
            dtTemp = ExecuteSqlQueryToDataTable(sSQL,dataTableName);

            // If no data table returned, bail out now. Error info will have already been set.
            if (dtTemp == null)
                return null;
            else
                // Export to list and return 
                return ConvertDataTableToList(dtTemp, firstRowColumnNames);
        }
        catch (Exception ex)
        {
            _LastJsonResponse = GetLastJsonResponse();
            _LastError = ex.Message;
            return null;
        }
    }

    /// <summary>
    ///  This function queries the DB2 database with selected SQL statement and returns results to XML dataset stream all in one step 
    ///  </summary>
    ///  <param name="sSQL">SQL Select. INSERT, UPDATE and DELETE not allowed </param>
    ///  <param name="sOutputFile">Optional PC output file for XML response data. Otherwise data set is created from memory.</param>
    ///  <returns>XML string</returns>
    ///  <remarks></remarks>
    public string ExecuteSqlQueryToXmlString(string sSQL, string sTableName = "Table1", bool bWriteSchema = false)
    {
         
        string sRtnXML = "";
        bool rtnquery;
        DataTable _dt;

        try
        {
            _LastError = "";
            sRtnXML = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // if table not set, default to Table1
            if (sTableName.Trim() == "")
                sTableName = "Table1";

            // Run query and load internal results DataTable
            rtnquery = ExecuteSqlQuery(sSQL,sTableName);

            // Export DataTable results to XML
            if (rtnquery)
            {
                StringBuilder SB = new StringBuilder();
                System.IO.StringWriter SW = new System.IO.StringWriter(SB);
                _dt = GetQueryResultsDataTable();
                _dt.TableName = sTableName;
                // Write XMl with or without schema info
                if (bWriteSchema)
                    _dt.WriteXml(SW, System.Data.XmlWriteMode.WriteSchema);
                else
                    _dt.WriteXml(SW);
                sRtnXML = SW.ToString();
                SW.Close();
                return sRtnXML;
            }
            else
                throw new Exception("Query failed. Error: " + GetLastError());
        }
        catch (Exception ex)
        {
            _LastJsonResponse = GetLastJsonResponse();
            _LastError = ex.Message;
            return "";
        }
    }

    /// <summary>
    ///  This function queries the DB2 database with selected SQL statement and returns results to XML file all in one step 
    ///  </summary>
    ///  <param name="sSQL">SQL Select. INSERT, UPDATE and DELETE not allowed </param>
    ///  <param name="sOutputFile">Optional PC output file for XML response data. Otherwise data set is created from memory.</param>
    ///  <returns>True - Query service call succeeded, False - Query service call failed</returns>
    ///  <remarks></remarks>
    public bool ExecuteSqlQueryToXmlFile(string sSQL, string sXmlOutputFile, bool replace = false, string sTableName = "Table1", bool bWriteSchema = false,string dataTableName= "Table1")
    {
         
        bool rtnquery;

        try
        {
            _LastError = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // Run query and load internal results DataTable
            rtnquery = ExecuteSqlQuery(sSQL,dataTableName);

            // Export results to XML file
            if (rtnquery)
                return GetQueryResultsDataTableToXmlFile(sXmlOutputFile, sTableName, bWriteSchema, replace);
            else
                throw new Exception("Query failed. Error: " + GetLastError());
        }
        catch (Exception ex)
        {
            _LastJsonResponse = GetLastJsonResponse();
            _LastError = ex.Message;
            return false;
        }
    }

    /// <summary>
    ///  This function queries the DB2 database with selected SQL statement and returns results to Csv string in one step
    ///  </summary>
    ///  <param name="sSQL">SQL Select. INSERT, UPDATE and DELETE not allowed </param>
    ///  <param name="sFieldSepchar">Field delimiter/separator. Default = Comma</param>
    ///  <param name="sFieldDataDelimChar">Field data delimiter character. Default = double quotes.</param>
    ///  <returns>CSV string</returns>
    public string ExecuteSqlQueryToCsvString(string sSQL, string sFieldSepchar = ",", string sFieldDataDelimChar = "\"",string dataTableName="Table1")
    {
         
        bool rtnquery;

        try
        {
            _LastError = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // Run query and load internal results DataTable
            rtnquery = ExecuteSqlQuery(sSQL,dataTableName);

            // Export results to CSV string
            if (rtnquery)
                return GetQueryResultsDataTableToCsvString(sFieldSepchar, sFieldDataDelimChar);
            else
                throw new Exception("Query failed. Error: " + GetLastError());
        }
        catch (Exception ex)
        {
            _LastJsonResponse = GetLastJsonResponse();
            _LastError = ex.Message;
            return "";
        }
    }

    /// <summary>
    ///  This function queries the DB2 database with selected SQL statement and returns results to Csv file in one step
    ///  </summary>
    ///  <param name="sSQL">SQL Select. INSERT, UPDATE and DELETE not allowed </param>
    ///  <param name="sCsvOutputFile">Output CSV file</param>
    ///  <param name="replace">Replace output file True=Replace file,False=Do not replace</param>
    ///  <param name="sFieldSepchar">Field delimiter/separator. Default = Comma</param>
    ///  <param name="sFieldDataDelimChar">Field data delimiter character. Default = double quotes.</param>
    ///  <returns>True-Query service call succeeded, False-Query service call failed</returns>
    public bool ExecuteSqlQueryToCsvFile(string sSQL, string sCsvOutputFile, bool replace = false, string sFieldSepchar = ",", string sFieldDataDelimChar = "\"", string dataTableName = "Table1")
    {
         
        bool rtnquery;

        try
        {
            _LastError = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // Run query and load internal results DataTable
            rtnquery = ExecuteSqlQuery(sSQL,dataTableName);

            // Export results to CSV file
            if (rtnquery)
                return GetQueryResultsDataTableToCsvFile(sCsvOutputFile, sFieldSepchar, sFieldDataDelimChar, replace);
            else
                throw new Exception("Query failed. Error: " + GetLastError());
        }
        catch (Exception ex)
        {
            _LastJsonResponse = GetLastJsonResponse();
            _LastError = ex.Message;
            return false;
        }
    }


    /// <summary>
    ///  This function queries the DB2 database with selected SQL statement and returns results to JSON string in one step
    ///  </summary>
    ///  <param name="sSQL">SQL Select. INSERT, UPDATE and DELETE not allowed </param>
    ///  <param name="sQueryResultOutputFile">Optional PC output file for XML response data. Otherwise data set is created from memory.</param>
    ///  <returns>JOSN string</returns>
    public string ExecuteSqlQueryToJsonString(string sSQL, string dataTableName = "Table1")
    {
         
        bool rtnquery;

        try
        {
            _LastError = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // Run query and load internal results DataTable
            rtnquery = ExecuteSqlQuery(sSQL,dataTableName);

            // Export results to CSV string
            if (rtnquery)
                return GetQueryResultsDataTableToJsonString();
            else
                throw new Exception("Query failed. Error: " + GetLastError());
        }
        catch (Exception ex)
        {
            _LastJsonResponse = GetLastJsonResponse();
            _LastError = ex.Message;
            return "";
        }
    }

    /// <summary>
    ///  This function queries the DB2 database with selected SQL statement and returns results to JSON file in one step
    ///  </summary>
    ///  <param name="sSQL">SQL Select. INSERT, UPDATE and DELETE not allowed </param>
    ///  <param name="sJsonOutputFile">Output JSON file</param>
    ///  <param name="replace">Replace output file True=Replace file,False=Do not replace</param>
    ///  <param name="sQueryResultOutputFile">Optional PC output file for XML response data. Otherwise data set is created from memory.</param>
    ///  <returns>True-Query service call succeeded, False-Query service call failed</returns>
    public bool ExecuteSqlQueryToJsonFile(string sSQL, string sJsonOutputFile, bool replace = false, string dataTableName = "Table1")
    {
         
        bool rtnquery;

        try
        {
            _LastError = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // Run query and load internal results DataTable
            rtnquery = ExecuteSqlQuery(sSQL,dataTableName);

            // Export results to JSON file
            if (rtnquery)
                return GetQueryResultsDataTableToJsonFile(sJsonOutputFile, replace);
            else
                throw new Exception("Query failed. Error: " + GetLastError());
        }
        catch (Exception ex)
        {
            _LastJsonResponse = GetLastJsonResponse();
            _LastError = ex.Message;
            return false;
        }
    }

    /// <summary>
    ///  This function runs the specified IBM i CL command line. The CL command can be a regular program call or a SBMJOB type of command to submit a job.
    ///  </summary>
    ///  <param name="commandString">CL command line to execute</param>
    ///  <returns>True - Command call succeeded, False - Command call failed</returns>
    ///  <remarks></remarks>
    public bool ExecuteCommand(string commandString)
    {
        string rtnJson = "";

        try
        {
            _LastError = "";
            rtnJson = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // Build CL command post
            string jsonPostData = @"{""command"":""@@command""}";
            jsonPostData = jsonPostData.Replace("@@command", commandString);

            // Execute POST request
            _LastJsonResponse = ExecuteHttpJsonPostRequest(_ServiceURL.Trim() + "/cl", jsonPostData, _HttpTimeout, _UseHttpCredentials, false, _encodeAuthBase64, _allowInvalidSslCertificates);

            // Check call success
            if (_LastJsonResponse.Contains("\"success\":false"))
            {
                throw new Exception(GetJsonValueAsString(_LastJsonResponse,"code") + " - Command call failed.");
            }
            else if (_LastJsonResponse.Contains("\"success\":true"))
            {
                return true;
            }
            else
            {
                throw new Exception("Command call failed. Invalid or unexpected response returned from web service.");
            }

        }
        catch (Exception ex)
        {
            _LastJsonResponse = rtnJson;
            _LastError = ex.Message;
            return false;
        }
    }

        /// <summary>
        ///  This function runs the specified IBM i Qshell command line. 
        ///  **Note: Qshell command call does not seem to be functional as of 12/16/2018
        ///  </summary>
        ///  <param name="commandString">Qshell command line to execute</param>
        ///  <returns>True - Command call succeeded, False - Command call failed</returns>
        ///  <remarks></remarks>
        public bool ExecuteQshellCommand(string commandString)
        {
            string rtnJson = "";

            try
            {
                _LastError = "";
                rtnJson = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build CL command post
                string jsonPostData = @"{""command"":""@@command""}";
                jsonPostData = jsonPostData.Replace("@@command", commandString);

                // Execute POST request
                _LastJsonResponse = ExecuteHttpJsonPostRequest(_ServiceURL.Trim() + "/qsh", jsonPostData, _HttpTimeout, _UseHttpCredentials, false, _encodeAuthBase64, _allowInvalidSslCertificates);

                // Check call success
                if (_LastJsonResponse.Contains("\"success\":false"))
                {
                    throw new Exception("Qshell command call failed.");
                }
                else if (_LastJsonResponse.Contains("\"success\":true"))
                {
                    return true;
                }
                else
                {
                    throw new Exception("Qshell command call failed. Invalid or unexpected response returned from web service.");
                }

            }
            catch (Exception ex)
            {
                _LastJsonResponse = rtnJson;
                _LastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        ///  This function will run the selected ILEusion API call with 
        ///  a formatted JSON packet provided by the caller rather than 
        ///  using the built in convenience methods. This allows for the 
        ///  flexibility of using the .Net wrapper right away without updating 
        ///  the assembly with new convenience methods if new APIs are added to ILEusion.
        ///  </summary>
        ///  <param name="jsonRequestData">Properly formatted ILEusion JSON request data string</param>
        ///  <param name="apiName">ILEusion API to call: /transaction, /sql, /call, /dq/send, /dq/pop, /cl, /qsh or any new api action added to ILEusion. 
        ///  Can also pass api name without forward slash and it will get handled. Ex: cl, sql, etc.</param>
        ///  <returns>JSON response data or string starting with ERROR: if an exception occurs.</returns>
        ///  <remarks></remarks>
        public string ExecuteJsonApiCall(string jsonRequestData,string apiName)
    {

        try
        {
            _LastError = "";
            _LastHttpStatusResponse = "";
            _LastJsonResponse = "";

            // Make sure request data passed in
            if (jsonRequestData.Trim() == "")
            {
               throw new Exception("No ILEusion JSON request data passed. API call cancelled.");
            }

            // Request type if required
            if (apiName.Trim() == "")
            {
                throw new Exception("No ILEusion API name passed. API call cancelled.");
            }
        
            // Request type if required
            if (apiName.Trim().StartsWith("/") == false)
            {
               apiName = "/" + apiName.Trim();
            }

            // Execute POST request with JSON request
            _LastJsonResponse = ExecuteHttpJsonPostRequest(_ServiceURL.Trim() + apiName.ToLower().Trim(),jsonRequestData, _HttpTimeout, _UseHttpCredentials, false, _encodeAuthBase64, _allowInvalidSslCertificates);

            return _LastJsonResponse;  

        }
        catch (Exception ex)
        {
            _LastJsonResponse = "ERROR: " + ex.Message;
            _LastError = ex.Message;
            return _LastJsonResponse;
        }
    }

    /// <summary>
    ///  Create fixed length flat physical file using SQL with long or short name. 
    ///  Data field name will be RECORD.
    ///  </summary> 
    ///  <param name="sTableName">SQL table name. Up to 30 characters.</param>
    ///  <param name="sTableLibrary">SQL table schema/library</param>
    ///  <param name="iRecordLength">Record length. Default=400</param>
    ///  <returns>True-Success, False-Error</returns>
    public bool CreateSqlTableFixed(string sTableName, string sTableLibrary, int iRecordLength = 400)
    {
        bool rtncmd = true;

        try
        {

            // Table creation command
            string sSqlCmd = string.Format("CREATE TABLE {0}/{1} (RECORD CHAR ({2}) NOT NULL WITH DEFAULT)", sTableLibrary.Trim().ToUpper(), sTableName.Trim().ToUpper(), iRecordLength);

            // Call SQL create command
            rtncmd = ExecuteSqlNonQuery(sSqlCmd);

            if (rtncmd)
                _LastError = string.Format("Table {0} was created in library {1}.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());
            else
                _LastError = string.Format("Errors occurred. It's possible table {0} in library {1} already exists.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());

            return rtncmd;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }
    /// <summary>
    ///  Insert record into fixed length flat physical file using SQL.
    ///  Data field name will be RECORD.
    ///  </summary> 
    ///  <param name="sTableName">SQL table name. Up to 30 characters.</param>
    ///  <param name="sTableLibrary">SQL table schema/library</param>
    ///  <param name="sRecordData">Single field data record</param>
    ///  <returns>True-Success, False-Error</returns>
    public bool InsertSqlTableFixed(string sTableName, string sTableLibrary, string sRecordData)
    {
        bool rtncmd = true;

        try
        {

            // Double up any single quoted records
            sRecordData = sRecordData.Replace("'", "''");

            string sSqlCmd = string.Format("INSERT INTO {0}/{1} (RECORD) VALUES('{2}')", sTableLibrary.Trim().ToUpper(), sTableName.Trim().ToUpper(), sRecordData);

            // Call SQL command
            rtncmd = ExecuteSqlNonQuery(sSqlCmd);

            if (rtncmd)
                _LastError = string.Format("Record inserted to Table {0} in library {1}.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());
            else
                _LastError = string.Format("Errors occurred. It's possible table {0} in library {1} does not exist or there were unpaired single quotes.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());

            return rtncmd;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }
    /// <summary>
    ///  Delete table using SQL DROP TABLE action
    ///  </summary> 
    ///  <param name="sTableName">SQL table name. Up to 30 characters.</param>
    ///  <param name="sTableLibrary">SQL table schema/library</param>
    ///  <returns>True-Success, False-Error</returns>
    public bool DeleteSqlTable(string sTableName, string sTableLibrary)
    {
        bool rtncmd = true;

        try
        {

            // SQL command
            string sSqlCmd = string.Format("DROP TABLE {0}/{1}", sTableLibrary.Trim().ToUpper(), sTableName.Trim().ToUpper());

            // Call SQL command
            rtncmd = ExecuteSqlNonQuery(sSqlCmd);

            if (rtncmd)
                _LastError = string.Format("Table {0} was deleted from library {1}.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());
            else
                _LastError = string.Format("Errors occurred. It's possible table {0} in library {1} does not exist.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());

            return rtncmd;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }
    /// <summary>
    ///  Clear table by Deleting all records from table using SQL DELETE action
    ///  </summary> 
    ///  <param name="sTableName">SQL table name. Up to 30 characters.</param>
    ///  <param name="sTableLibrary">SQL table schema/library</param>
    ///  <returns>True-Success, False-Error</returns>
    public bool ClearSqlTable(string sTableName, string sTableLibrary)
    {
        bool rtncmd = true;

        try
        {

            // SQL command
            string sSqlCmd = string.Format("DELETE FROM {0}/{1}", sTableLibrary.Trim().ToUpper(), sTableName.Trim().ToUpper());

            // Call SQL command
            rtncmd = ExecuteSqlNonQuery(sSqlCmd);

            if (rtncmd)
                _LastError = string.Format("Records were deleted from Table {0} in library {1}.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());
            else
                _LastError = string.Format("Errors occurred. It's possible table {0} in library {1} does not exist.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());

            return rtncmd;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }
    /// <summary>
    ///  Does SQL table exist ? We check SYSTABLES in QSYS2 for table existence.
    ///  Note: Only works for DDS defined or SQL defined tables. Flat files created with CRTPF will not show up.
    ///  </summary> 
    ///  <param name="sTableName">SQL table name. Up to 30 characters.</param>
    ///  <param name="sTableLibrary">SQL table schema/library</param>
    ///  <returns>True-Exists, False-Does not exist or rrror</returns>
    public bool CheckSqlTableExists(string sTableName, string sTableLibrary)
    {
        DataTable dtWork;
        string sql = "";

        try
        {

            // Build table check for SQL table
            sql = string.Format("SELECT COUNT(*) as TABLECOUNT From QSYS2/SYSTABLES WHERE TABLE_SCHEMA='{0}' and TABLE_NAME='{1}'", sTableLibrary.Trim(), sTableName.Trim());

            // Run the table check query
            dtWork = ExecuteSqlQueryToDataTable(sql);

            if (dtWork == null)
                throw new Exception("SQL error occurred.");
            else
                // Should only ever get a single count result row
                if (dtWork.Rows.Count == 1)
            {
                // Check the count to see if we found the table using SYSTABLES
                if (Convert.ToInt32(dtWork.Rows[0]["TABLECOUNT"]) > 0)
                {
                    _LastError = string.Format("Table {0} in library {1} exists.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());
                    return true;
                }
                else
                {
                    _LastError = string.Format("Table {0} in library {1} does not exist or is possibly a flat file created with CRTPF so not in SYSTABLES.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());
                    return false;
                }
            }
            else
            {
                _LastError = string.Format("Error occurred. Only 1 count row expected.", sTableName.Trim().ToUpper(), sTableLibrary.Trim().ToUpper());
                return false;
            }
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return false;
        }
    }

        /// <summary>
        ///  Make an HTTP JSON POST request to ILEastic/ILEusion service with selected URL and get response data. 
        ///  </summary>
        ///  <param name="url">URL where ILEusion/ILEastic service is set up.</param>
        ///  <param name="jsonPostData">JSON request data to post with the request</param>
        ///  <param name="httpTimeout">Optional HTTP request timeout. Default = 10000 milliseconds</param>
        ///  <param name="useHttpCredentials">Optional Use network credentials for web server auth. 0=No, 1=Yes Default = 0</param>
        ///  <param name="allowInvalidSslCertificates">Optional Allow invallid certs. true=Yes, false=no. Default=false - certs must be valid.</param>
        ///  <returns>JSON response or error string starting with "ERROR" </returns>
        ///  <remarks></remarks>
        public string ExecuteHttpJsonPostRequest(string url, string jsonPostData, int httpTimeout = 10000, bool useHttpCredentials = false, bool encodeData = false,bool encodeAuthBase64=false,bool allowInvalidSslCertificates = false)
        {
        string responseData = "";
        try
        {

            // Set TLS mode to 1.2 if not set and also set allow invalid certificates setting value.   
            if (System.Net.ServicePointManager.SecurityProtocol != System.Net.SecurityProtocolType.Tls12) {
                    
                    // Set TLS 1.2 protocal 
                    System.Net.ServicePointManager.SecurityProtocol = System.Net.SecurityProtocolType.Tls12;

                    // If enabled, this callback allows us to ignore invalid certificates. 
                    // https://stackoverflow.com/questions/2675133/c-sharp-ignore-certificate-errors
                    if (allowInvalidSslCertificates)
                    {
                        System.Net.ServicePointManager.ServerCertificateValidationCallback += (sender, cert, chain, sslPolicyErrors) => true;
                    }
            }

            _LastHttpStatusResponse = "";
            _LastHttpResponseData = "";
            System.Net.HttpWebRequest hwrequest = (System.Net.HttpWebRequest) System.Net.WebRequest.Create(url);
            hwrequest.Accept = "*/*";
            hwrequest.AllowAutoRedirect = true;

                // If specified use ILEastic http service basic authentication, set basic auth header
                if (useHttpCredentials) {
                    if (encodeAuthBase64)
                    {
                        hwrequest.Headers.Add("Authorization", "Basic " +  EncodeStringToBase64(_HttpUser + ":" + _HttpPassword));
                    }
                    else
                    {
                        hwrequest.Headers.Add("Authorization", "Basic " + _HttpUser + ":" + _HttpPassword);
                    }
            }

            hwrequest.UserAgent = "ILEusion/1.0";
            hwrequest.Timeout = httpTimeout;
            hwrequest.Method = "POST";
            //hwrequest.Headers.Add("Transfer-Encoding", "chunked"); // Causes error. Don't use this parm. Postman does chunked posts.

            // Set JSON content type for POST request 
            hwrequest.ContentType = "application/json;charset=UTF-8";

            // If enabled, encode the URL post data before posting. Probably not necessary for ILEastic/ILEusion.
            if (encodeData)
            {
                jsonPostData = EncodeUrl(jsonPostData);
            }

            // Use UTF8Encoding for requests. I believe ILEastic uses UTF8.
            // This chunk encodes our request data
            System.Text.UTF8Encoding encoding = new System.Text.UTF8Encoding(); 
            byte[] postByteArray = encoding.GetBytes(jsonPostData);
            hwrequest.ContentLength = postByteArray.Length;
            System.IO.Stream postStream = hwrequest.GetRequestStream();
            postStream.Write(postByteArray, 0, postByteArray.Length);
            postStream.Close();

            // Make the HTTP request 
            System.Net.HttpWebResponse hwresponse = (System.Net.HttpWebResponse) hwrequest.GetResponse();
            
            // Save last response info
            _LastHttpStatusResponse = hwresponse.StatusCode + " " + hwresponse.StatusDescription;

            // Process the response if all is good
            if (hwresponse.StatusCode == System.Net.HttpStatusCode.OK)
            {
                // Get response date from the HTTP response so we can use it.
                System.IO.StreamReader responseStream = new System.IO.StreamReader(hwresponse.GetResponseStream());
                responseData = responseStream.ReadToEnd();
                _LastHttpResponseData = responseData;
            }
            // Close the reponse
            hwresponse.Close();
            }
            catch (Exception e)
            {
                responseData = "ERROR - An HTTP error occurred: " + e.Message;
                _LastHttpResponseData = responseData;
            }
            // Return the response data
            return responseData;
        }

        #endregion

        #region "IBMi DataQueue Methods"

        /// <summary>
        ///  This function sends an entry to selected data queue.
        ///  </summary>
        /// <param name="dataQueueName">Data queue name</param>
        /// <param name="dataQueueLibrary">Data queue library</param>
        /// <param name="dataVale">Data value to send</param>
        /// <returns>True=Success, False=Error</returns>
        public bool SendToDataQueue(string dataQueueName,string dataQueueLibrary,string dataValue)
        {

            try
            {
                _LastError = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build JSON request
                string jsonPostData = @"{""library"":""@@dqlibrary"",""object"":""@@dqname"",""data"":""@@data""}";
                jsonPostData = jsonPostData.Replace("@@data", dataValue);
                jsonPostData = jsonPostData.Replace("@@dqlibrary", dataQueueLibrary.Trim().ToUpper());
                jsonPostData = jsonPostData.Replace("@@dqname", dataQueueName.Trim().ToUpper());

                // Execute POST request
                _LastJsonResponse = ExecuteHttpJsonPostRequest(_ServiceURL.Trim() + "/dq/send", jsonPostData, _HttpTimeout, _UseHttpCredentials,false,_encodeAuthBase64,_allowInvalidSslCertificates);

                // Check data queue success
                if (_LastJsonResponse.Contains("\"success\":true")) // Send successful
                {
                    return true;
                }
                else if (_LastJsonResponse.Contains("\"success\":false")) // Bad send but service active
                {
                    throw new Exception("Data queue send failed.");
                }
                else // Bad send. Service or connection may be down.
                {
                    throw new Exception("Data queue send failed. Connection may not be active or service down.");
                }

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        ///  This function receives an entry from selected data queue.
        ///  </summary>
        /// <param name="dataQueueName">Data queue name</param>
        /// <param name="dataQueueLibrary">Data queue library</param>
        /// <returns>Data value or blank if no data returned.</returns>
        public string ReceiveFromDataQueue(string dataQueueName, string dataQueueLibrary)
        {

            try
            {
                _LastError = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build JSON request
                string jsonPostData = @"{""library"":""@@dqlibrary"",""object"":""@@dqname""}";
                jsonPostData = jsonPostData.Replace("@@dqlibrary", dataQueueLibrary.Trim().ToUpper());
                jsonPostData = jsonPostData.Replace("@@dqname", dataQueueName.Trim().ToUpper());

                // Execute POST request
                _LastJsonResponse = ExecuteHttpJsonPostRequest(_ServiceURL.Trim() + "/dq/pop", jsonPostData, _HttpTimeout, _UseHttpCredentials, false, _encodeAuthBase64, _allowInvalidSslCertificates);

                // Check data queue success
                if (_LastJsonResponse.Contains("\"success\":true")) // Send successful
                {
                    return GetJsonValueAsString(_LastJsonResponse,"value");
                }
                else if (_LastJsonResponse.Contains("\"success\":false")) // Bad send but service active
                {
                    throw new Exception("Data queue receive failed.");
                }
                else // Bad send. Service or connection may be down.
                {
                    throw new Exception("Data queue receive failed. Connection may not be active or service down.");
                }

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return "";
            }
        }

        /// <summary>
        ///  This function deletes a data queue if it exists.
        ///  </summary>
        /// <param name="dataQueueName">Data queue name</param>
        /// <param name="dataQueueLibrary">Data queue library</param>
        /// <returns>True=Success, False=Error</returns>
        public bool DeleteDataQueue(string dataQueueName, string dataQueueLibrary)
        {

            try
            {
                _LastError = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build CL command string
                string command = string.Format(@"DLTDTAQ DTAQ({0}/{1})", dataQueueLibrary.Trim().ToUpper(),dataQueueName.Trim().ToUpper());

                // Execute command
                return ExecuteCommand(command);

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        ///  This function creates a sequential non keyed data queue if it does not exist.
        ///  </summary>
        /// <param name="dataQueueName">Data queue name</param>
        /// <param name="dataQueueLibrary">Data queue library</param>
        /// <param name="recordLength">Max length of data record</param>
        /// <returns>True=Success, False=Error</returns>
        public bool CreateDataQueue(string dataQueueName, string dataQueueLibrary,int recordLength=100)
        {

            try
            {
                _LastError = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build CL command string
                string command = string.Format(@"CRTDTAQ DTAQ({0}/{1}) MAXLEN({2}) SEQ(*FIFO)", dataQueueLibrary.Trim().ToUpper(), dataQueueName.Trim().ToUpper(),recordLength);

                // Execute command
                return ExecuteCommand(command);

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }
        }

        #endregion

        #region "IBMi General CL Command Convenience Methods"

        /// <summary>
        ///  This function checks for IBM i object existence.
        ///  Convenience method for CHKOBJ command.
        ///  If object exists, no CPF errors, so true should be the return. 
        ///  Any error that occurs will cause a false return code since we 
        ///  don't have access to CPF return messages. 
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <param name="objectLibrary">Object library</param>
        /// <param name="objectType">Object type</param>
        /// <param name="member">Member name if database file. Optional. Default=*NONE</param>
        /// <param name="authority">Authority if database file. Optional. Default = *NONE</param>
        /// <returns>True=object exists, False=Object not found or other error 
        /// since we don't have access to CPF message results from command call.</returns>
        public bool CheckObjectExists(string objectName, string objectLibrary, string objectType, string member = "*NONE", string authority = "*NONE")
        {

            try
            {
                _LastError = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build CL command string
                string command = string.Format(@"CHKOBJ OBJ({0}/{1}) OBJTYPE({2}) MBR({3}) AUT({4})", objectLibrary.Trim().ToUpper(), objectName.Trim().ToUpper(), objectType.Trim().ToUpper(), member.Trim().ToUpper(), authority.Trim().ToUpper());

                // Execute command
                return ExecuteCommand(command);

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        ///  This function sends a message to the selected user.
        ///  Convenience method for SNDMSG command.
        /// </summary>
        /// <param name="message">Message text </param>
        /// <param name="toUser">Recipient user ID</param>
        /// <returns></returns>
        public bool SendMessage(string message, string toUser)
        {

            try
            {
                _LastError = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build CL command string
                string command = string.Format(@"SNDMSG MSG('{0}') TOUSR({1})",message,toUser.Trim().ToUpper());

                // Execute command
                return ExecuteCommand(command);

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        ///  This function creates a fixed length flat physical file in the 
        ///  specified IBM i library. Used for temp file creation.
        ///  Convenience method for CRTPF command to create flat file without DDS.
        ///  Note: SQL can also be used to create temp tables as well.
        /// </summary>
        /// <param name="fileName">File name. 10 characters.</param>
        /// <param name="fileLibrary">File library name. 10 characters</param>
        /// <param name="recordLength">Record length. Optional.Default=1024</param>
        /// <param name="fileDescription">Text description for table. Optional. Default=blank.</param>
        /// <param name="authority">Object authority. Optional. Default=*LIBCRTAUT</param>
        /// <returns>True=File created. False=File not created.</returns>
        public bool CreateFixedLengthPhysicalFile(string fileName, string fileLibrary,int recordLength=1024,string fileDescription="",string authority="LIBCRTAUT")
        {

            try
            {
                _LastError = "";
                _LastHttpStatusResponse = "";
                _LastJsonResponse = "";

                // Build CL command string
                string command = String.Format("CRTPF FILE({1}/{0}) RCDLEN({2}) TEXT('{3}') OPTION(*NOSRC *NOLIST *NOSECLVL) MAXMBRS(1) SIZE(*NOMAX) AUT({4})",fileName.Trim().ToUpper(), fileLibrary.Trim().ToUpper(),recordLength,fileDescription.Trim(),authority.Trim().ToUpper());

                // Execute command
                return ExecuteCommand(command);

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }
        }

        #endregion

        #region "General Utility Methods"

        /// <summary>
        /// Encode string to Base64 string
        /// </summary>
        /// <param name="valueToEncode">String to encode</param>
        /// <returns>Encoded string or blanks on error</returns>
        static public string EncodeStringToBase64(string valueToEncode)
        {

            try
            {
                byte[] toEncodeAsBytes
                      = System.Text.ASCIIEncoding.ASCII.GetBytes(valueToEncode);
                string returnValue
                      = System.Convert.ToBase64String(toEncodeAsBytes);
                return returnValue;
            } catch (Exception)
            {
                return "";
            }
        }
        /// <summary>
        /// Decode Base64 encoded string
        /// </summary>
        /// <param name="encodedData">Encoded string</param>
        /// <returns>Deconded string or blanks on error</returns>
        static public string DecodeStringFromBase64(string encodedData)
        {

            try
            {
            byte[] encodedDataAsBytes
                = System.Convert.FromBase64String(encodedData);
            string returnValue =
               System.Text.ASCIIEncoding.ASCII.GetString(encodedDataAsBytes);
            return returnValue;
            } catch (Exception)
            {
                return "";
            }

        }

        /// <summary>
        ///  Encode URL string
        ///  </summary>
        ///  <param name="sURL">URL to encode</param>
        ///  <returns>Encoded URL string</returns>
        ///  <remarks></remarks>
        public string EncodeUrl(string sURL)
    {
        try
        {
            _LastError = "";
            sURL = System.Web.HttpUtility.UrlEncode(sURL);
            return sURL;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            // Return original URL
            return sURL;
        }
    }

    /// <summary>
    ///  Convert DataTable Row List to Generic List and optionally include column names.
    ///  </summary>
    ///  <param name="dtTemp">DataTable Object</param>
    ///  <param name="firstRowColumnNames">Optional - Return first row as column names. False=No column names, True=Return column names. Default=False</param>
    ///  <returns>List object</returns>
    public List<List<object>> ConvertDataTableToList(DataTable dtTemp, bool firstRowColumnNames = false)
    {
        List<List<object>> result = new List<List<object>>();
        List<object> values = new List<object>();

        try
        {
            _LastError = "";

            // Include first row as columns
            if (firstRowColumnNames)
            {
                foreach (DataColumn column in dtTemp.Columns)
                    values.Add(column.ColumnName);
                result.Add(values);
            }

            // Output all the data now
            foreach (DataRow row in dtTemp.Rows)
            {
                values = new List<object>();
                foreach (DataColumn column in dtTemp.Columns)
                {
                    if (row.IsNull(column))
                        values.Add(null);
                    else
                        values.Add(row[column]);
                }
                result.Add(values);
            }
            return result;
        }
        catch (Exception ex)
        {
            _LastError = ex.Message;
            return null;
        }
    }

        /// <summary>
        ///  Convert JSON to DataTable by deserializing JSON data
        ///  Note: The assumption is a simple JSON return array for this to work.
        ///  Complex JSON may not convert correctly
        ///  </summary>
        ///  <param name="jsonData">JSON data. </param>
        ///  <returns>DataTable object or null on error</returns>
        public DataTable ConvertJsonToDataTable(string jsonData,string dataTableName="Table1")
        {

            try
            {
                _LastError = "";

                // Deserialize JSON to a DataTable
                var table = JsonConvert.DeserializeObject<DataTable>(jsonData);
                table.TableName = dataTableName;
                return table;

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return null;
            }
        }

        /// <summary>
        ///  Convert JSON to List by deserializing JSON data
        ///  Note: The assumption is a simple JSON return array for this to work.
        ///  Complex JSON may not convert correctly
        ///  </summary>
        ///  <param name="jsonData">JSON data. </param>
        ///  <param name="firstRowColumnNames">Optional - Return first row as column names. False=No column names, True=Return column names. Default=False</param>
        ///  <returns>List object</returns>
        public List<List<object>> ConvertJsonToList(string jsonData,bool firstRowColumnNames=false)
        {

            try
            {
                _LastError = "";

                // Deserialize JSON to a DataTable
                var table = JsonConvert.DeserializeObject<DataTable>(jsonData);
                table.TableName = "Table1"; // Set tablename. Doesn't really matter for list return
                return ConvertDataTableToList(table,firstRowColumnNames);

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return null;
            }
        }


        /// <summary>
        /// Get single JSON token value as string
        /// </summary>
        /// <param name="jsonData">JSON data</param>
        /// <param name="selectTokenField">Token to select individual field value. Ex: disbursement[0] gives field 0 in an array of values. disbursements[0].status gives the status field in element 0.</param>
        /// <returns>Data value or blanks</returns>
        public string GetJsonValueAsString(string jsonData, string selectTokenField)
        {
            try
            {

                _LastError = "";

                // Parse JSON and look for selected value
                var jsonText = JObject.Parse(jsonData);
                var tokenField = (string)jsonText.SelectToken(selectTokenField);
                return tokenField.ToString();

                // Sample to Iterate tokens (saved for future use to iterate multiple values)
                //foreach (var selectedTokens in jsonText.SelectTokens(tokenSelect))
                //{
                //    return selectedTokens[fieldName].ToString();
                //}

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return "";
            }
        }

        /// <summary>
        /// Get single JSON token value as integer
        /// </summary>
        /// <param name="jsonData">JSON data</param>
        /// <param name="selectTokenField">Token to select individual field value. Ex: disbursement[0] gives field 0 in an array of values. disbursements[0].status gives the status field in element 0.</param>
        /// <returns>Integer value or 0 on error</returns>
        public int GetJsonValueAsInt(string jsonData, string selectTokenField)
        {
            try
            {

                _LastError = "";

                // Parse JSON and look for selected value
                var jsonText = JObject.Parse(jsonData);
                var tokenField = (int)jsonText.SelectToken(selectTokenField);
                return tokenField;

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return 0;
            }
        }

        /// <summary>
        /// Get single JSON token value as double
        /// </summary>
        /// <param name="jsonData">JSON data</param>
        /// <param name="selectTokenField">Token to select individual field value. Ex: disbursement[0] gives field 0 in an array of values. disbursements[0].status gives the status field in element 0.</param>
        /// <returns>Double value or -999999 on error</returns>
        public double GetJsonValueAsDouble(string jsonData, string selectTokenField)
        {
            try
            {

                _LastError = "";

                // Parse JSON and look for selected value
                var jsonText = JObject.Parse(jsonData);
                var tokenField = (double)jsonText.SelectToken(selectTokenField);
                return tokenField;

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return 0;
            }
        }

        /// <summary>
        /// Get single JSON token value as float
        /// </summary>
        /// <param name="jsonData">JSON data</param>
        /// <param name="selectTokenField">Token to select individual field value. Ex: disbursement[0] gives field 0 in an array of values. disbursements[0].status gives the status field in element 0.</param>
        /// <returns>Float value or 0 on error</returns>
        public float GetJsonValueAsFloat(string jsonData, string selectTokenField)
        {
            try
            {

                _LastError = "";

                // Parse JSON and look for selected value
                var jsonText = JObject.Parse(jsonData);
                var tokenField = (float)jsonText.SelectToken(selectTokenField);
                return tokenField;

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return 0;
            }
        }

        /// <summary>
        /// Get single JSON token value as bool
        /// </summary>
        /// <param name="jsonData">JSON data</param>
        /// <param name="selectTokenField">Token to select individual field value. Ex: disbursement[0] gives field 0 in an array of values. disbursements[0].status gives the status field in element 0.</param>
        /// <returns>Actual boolean or false on error.</returns>
        public bool GetJsonValueAsBool(string jsonData, string selectTokenField)
        {
            try
            {

                _LastError = "";

                // Parse JSON and look for selected value
                var jsonText = JObject.Parse(jsonData);
                var tokenField = (bool)jsonText.SelectToken(selectTokenField);
                return tokenField;

            }
            catch (Exception ex)
            {
                _LastError = ex.Message;
                return false;
            }
        }

        #endregion



    }

}
