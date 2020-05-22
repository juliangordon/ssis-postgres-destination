/*
	-- =============================================
	-- Author:		Julian Gordon
	-- Create date: 07 October 2019
	-- Description:	Postgres SSIS Destination Component
	--
	--
	-- History
	-- 07 Oct 2019	    Julian Gordon		Version 1.0.0.0 Initial version of component.
    -- 12 Nov 2019      Julian Gordon       Version 1.0.0.1 Removed batch size property, as it's not used.
    --                                      Batch Size and Partition properties are not used (Deprecated). 
	-- =============================================
	*/

using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;

namespace SSIS.Postgres
{
   
    [DtsPipelineComponent
    (
     DisplayName = "PostgresQL Destination",
     CurrentVersion = 0,
     ComponentType = ComponentType.DestinationAdapter,
     Description = "Insert data stream into postgresql db",
     IconResource = "SSIS.Postgres.Icon2.ico"
    )]
    public class PostgresDestination : PipelineComponent
    {
        /*
        public override void PerformUpgrade(int pipelineVersion)
        {

            // Not implemented

        }*/

        #region variables

        #region design variables

        private const string COMPONENT_NAME = "PostgresQL Destination";
        private const string COMPONENT_DESCRIPTION = "Insert into a PostgresQL destination via NpgSQL";

        private const string CONNECTION_NAME = "ADO Net Connection";
        private const string CONNECTION_DESCRIPTION = "ADO Net Connection via NpgSQL";

        private const string INPUT_NAME = "Postgres Input";
        private const string INPUT_DESCRIPTION = "Postgres Input";
        private const string POSTGRES_TYPE = "PostgresType";

        private const string TABLE_NAME = "Table Name (Required)";
        private const string TABLE_DESCRIPTION = "Name of the table that the data will be inserted into";

        private const string PERFORM_AS_TRANSACTION_NAME = "Perform in single transaction";
        private const string PERFORM_AS_TRANSACTION_DESCRIPTION = "True: Commit at end.. False: Commit for each batch size";
        private const string PERFORM_POSTGRES_COPY = "Perform postgres copy";
        private const string PERFORM_POSTGRES_COPY_DESCRIPTION = "True: Performs postgres copy end.. False: Performs insert.";

        private NpgsqlConnection odpConnection;
        
        #endregion

        #region runtime variables

        private NpgsqlTransaction  odpTran;
        private NpgsqlCommand odpCmd;
        private NpgsqlParameter odpParam;
        private string odpCopyCommand;

        private string tableName;
        private bool performAsTransaction;
        private bool performPostgresCopy;

        private int[] inputIndexes;
        private List<ColumnInfo> columnInfos;

        private int colsCount;

        #endregion

        #endregion

        #region design time

        public override void ProvideComponentProperties()
        {
            base.ProvideComponentProperties();
            
            this.RemoveAllInputsOutputsAndCustomProperties();
            this.ComponentMetaData.RuntimeConnectionCollection.RemoveAll();

            this.ComponentMetaData.Name = COMPONENT_NAME;
            this.ComponentMetaData.Description = COMPONENT_DESCRIPTION;

            //Microsoft.SqlServer.Dts.Pipeline.embed
            /*
             * FAILURES:
             * System.MissingMethodException: Method not found 'Microsoft.SqlServer.Dts.Pipeline.Wrapper.IDTSComponentMetaData100 get_ComponentMetaData()'
             * 
             * Fix:
             * Please open your csproj file, and remove < EmbedInteropTypes > True </ EmbedInteropTypes > element or set the value to False in the reference to Microsoft.SQLServer.DTSPipelineWrap.dll , then rebuild your project.
             * 
             * Also removed below from .csproj file
             *<Reference Include="Microsoft.SqlServer.DTSPipelineWrap, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91, processorArchitecture=MSIL">
                  <EmbedInteropTypes>True</EmbedInteropTypes>
                </Reference>
                <Reference Include="Microsoft.SQLServer.DTSRuntimeWrap, Version=13.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91, processorArchitecture=x86">
                  <EmbedInteropTypes>False</EmbedInteropTypes>
                </Reference>
             * */

            this.GetRuntimeConnection();
            this.AddComponentInputs();
            this.EnableExternalMetadata();
            this.AddCustomProperties();

            /*
            This section is included to update version number automatically in case of changes
            made to the component.
            http://toddmcdermid.blogspot.com/2010/09/set-componentmetadataversion-in.html
            */
            /*DtsPipelineComponentAttribute componentAttribute =
            (DtsPipelineComponentAttribute)Attribute.GetCustomAttribute(this.GetType(),
            typeof(DtsPipelineComponentAttribute), false);
            ComponentMetaData.Version = componentAttribute.CurrentVersion;*/
        }

        //Setup NPGSQL Connection
        private void GetRuntimeConnection()
        {
            IDTSRuntimeConnection100 odpConn;
            try
            {
                odpConn = this.ComponentMetaData.RuntimeConnectionCollection[CONNECTION_NAME];
                
                FireEvent(EventType.Error, "Error in GetRuntimeConnection : " + odpConn.ConnectionManager.ConnectionString);
            }
            catch (Exception)
            {
                odpConn = this.ComponentMetaData.RuntimeConnectionCollection.New();
                odpConn.Name = CONNECTION_NAME;
                odpConn.Description = CONNECTION_DESCRIPTION;
            }
        }

        //Setup Postgres Input
        private void AddComponentInputs()
        {
            var input = this.ComponentMetaData.InputCollection.New();
            input.Name = INPUT_NAME;
            input.Description = INPUT_DESCRIPTION;
            input.HasSideEffects = true;
        }

        //Enable External Metadata -- Seen on Column mapping as the external columns
        private void EnableExternalMetadata()
        {
            var input = this.ComponentMetaData.InputCollection[INPUT_NAME];
            input.ExternalMetadataColumnCollection.IsUsed = true;
            this.ComponentMetaData.ValidateExternalMetadata = true;
        }

        private void AddCustomProperties()
        {
            Func<string, string, IDTSCustomProperty100> addProperty = delegate (string name, string description)
            {
                var property = this.ComponentMetaData.CustomPropertyCollection.New();
                property.Name = name;
                property.Description = description;
                property.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;

                return property;
            };

            addProperty(TABLE_NAME, TABLE_DESCRIPTION).Value = string.Empty;
            addProperty(PERFORM_AS_TRANSACTION_NAME, PERFORM_AS_TRANSACTION_DESCRIPTION).Value = (object)true;
            addProperty(PERFORM_POSTGRES_COPY, PERFORM_POSTGRES_COPY_DESCRIPTION).Value = (object)true;
        }

        public override void ReinitializeMetaData()
        {
            base.ReinitializeMetaData();

            var input = base.ComponentMetaData.InputCollection[INPUT_NAME];
            input.ExternalMetadataColumnCollection.RemoveAll();
            input.InputColumnCollection.RemoveAll();

            this.GenerateExternalColumns();

        }

        private void GenerateExternalColumns()
        {
            var tableName = this.GetMemberTableName();
            using (DataTable metaData = this.GetMemberTableMetaData(tableName))
            {
                if (metaData != null)
                {
                    var input = this.ComponentMetaData.InputCollection[INPUT_NAME];
                    var external = input.ExternalMetadataColumnCollection;

                    var count = metaData.Rows.Count;
                    for (int i = 0; i < count; i++)
                    {
                        var dataRow = metaData.Rows[i];
                        var externalColumn = external.NewAt(i);
                        externalColumn.Name = (string)dataRow["ColumnName"];
                        var type = (Type)dataRow["DataType"];
                        var dataType = PipelineComponent.DataRecordTypeToBufferType(type);
                        if ((bool)dataRow["IsLong"])
                        {
                            switch (dataType)
                            {
                                case DataType.DT_BYTES:
                                    dataType = DataType.DT_IMAGE;
                                    break;
                                case DataType.DT_STR:
                                    dataType = DataType.DT_TEXT;
                                    break;
                                case DataType.DT_WSTR:
                                    dataType = DataType.DT_NTEXT;
                                    break;
                            }
                        }
                        int length = 0;
                        if (dataType == DataType.DT_WSTR || dataType == DataType.DT_STR || dataType == DataType.DT_BYTES || dataType == DataType.DT_IMAGE || dataType == DataType.DT_NTEXT || dataType == DataType.DT_TEXT)
                        {
                            length = (int)dataRow["ColumnSize"];
                        }
                        int codePage = 0;
                        if (dataType == DataType.DT_STR)
                        {
                            codePage = -1;
                        }
                        int precision = 0;
                        int num = 0;
                                               
                        if (dataType == DataType.DT_NUMERIC || dataType == DataType.DT_DECIMAL)
                        {
                            if (dataRow.IsNull("NumericPrecision"))
                            {
                                precision = 29;
                            }
                            else
                            {
                                precision = (int)Convert.ToInt16(dataRow["NumericPrecision"]);
                            }
                            if (dataRow.IsNull("NumericScale"))
                            {
                                num = 0;
                            }
                            else
                            {
                                num = (int)Convert.ToInt16(dataRow["NumericScale"]);
                            }
                            if (num == 255)
                            {
                                num = 0;
                            }
                        }
                        externalColumn.DataType = dataType;
                        externalColumn.Length = length;
                        externalColumn.Precision = precision;
                        externalColumn.Scale = num;
                        externalColumn.CodePage = codePage;

                        IDTSCustomProperty100 propType = externalColumn.CustomPropertyCollection.New();
                        propType.Name = POSTGRES_TYPE;
                        propType.Value = dataRow["ProviderType"];
                    }
                }
            }
        }

        private string GetMemberTableName()
        {
            var tableName = ComponentMetaData.CustomPropertyCollection[TABLE_NAME].Value.ToString();
            if (string.IsNullOrEmpty(tableName))
            {
                FireEvent(EventType.Error, "Table name needs to be provided");
                throw new PipelineComponentHResultException(HResults.DTS_E_ADODESTFAILEDTOACQUIRECONNECTION);
            }
            return tableName;
        }

        private DataTable GetMemberTableMetaData(string tableName)
        {
            try
            {
                /*
                 * Obtains schema of table!
                 * */
                DataTable metaData = null;

                if (!string.IsNullOrEmpty(tableName))
                {
                    using (NpgsqlCommand cmd = odpConnection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = "SELECT * FROM " + tableName + " LIMIT 0;";

                        metaData = cmd.ExecuteReader(CommandBehavior.SchemaOnly).GetSchemaTable();
                    }
                }

                return metaData;
            }
            catch (Exception ex)
            {
                FireEvent(EventType.Error, "Error in GetMemberTableMetaData : " + ex.StackTrace);
                return null;
            }
            
            
        }

        #endregion

        #region runtime

        public override void PreExecute()
        {
            base.PreExecute();

            try
            {
                var input = base.ComponentMetaData.InputCollection[INPUT_NAME];
                colsCount = input.InputColumnCollection.Count;

                StoreInputColumns(input, colsCount);
                StoreUserVariables();

                odpCmd = new NpgsqlCommand();
                odpCmd.Connection = odpConnection;
                odpCmd.CommandType = CommandType.Text;
                
                if (performAsTransaction == true)
                {
                    odpTran = odpConnection.BeginTransaction();
                }

                string colsList = null;
                string paramsList = null;
                for (int colIndex = 0; colIndex <= colsCount - 1; colIndex++)
                {
                    var externalColumnName = columnInfos[colIndex].ExternalColumnName;
                    var externalDataType = columnInfos[colIndex].ColumnDataType;
                    
                    if (!string.IsNullOrEmpty(colsList))
                    {
                        colsList += ", ";
                        paramsList += ", ";
                    }
                    colsList += string.Format("\"{0}\"", externalColumnName);
                    paramsList += string.Format(":{0}", externalColumnName);

                    odpParam = new NpgsqlParameter(":" + externalColumnName, externalDataType);
                    odpCmd.Parameters.Add(odpParam);
                }

                if (!performPostgresCopy)
                {
                    // performs an insert
                    odpCmd.CommandText = string.Format("INSERT INTO {0} ( {1} ) VALUES ( {2} ); ", tableName, colsList, paramsList);
                }
                else
                {
                    // performs a postgres copy.
                    odpCopyCommand = string.Format("COPY {0} ({1}) FROM STDIN (FORMAT BINARY);", tableName, colsList);
                }
            }
            catch (Exception e)
            {
                FireEvent(EventType.Error, "Error in PreExecute : " + e.Message);
                if (performAsTransaction == true)
                {
                    if (!odpTran.IsCompleted)
                    {
                        odpTran.Commit();
                    }
                }
                throw (e);
            }
        }

        private static object GetBufferColumnValue(PipelineBuffer buffer, ColumnInfo col)
        {
            if (buffer.IsNull(col.BufferIndex))
                return null;

            switch (col.ColumnDataType)
            {
                case DataType.DT_BOOL:
                    return buffer.GetBoolean(col.BufferIndex);
                case DataType.DT_BYTES:
                    return buffer.GetBytes(col.BufferIndex);
                case DataType.DT_CY:
                    return buffer.GetDecimal(col.BufferIndex);
                case DataType.DT_DATE:
                    return buffer.GetDateTime(col.BufferIndex);
                case DataType.DT_DBDATE:
                    return buffer.GetDate(col.BufferIndex);
                case DataType.DT_DBTIME:
                    return buffer.GetTime(col.BufferIndex);
                case DataType.DT_DBTIME2:
                    return buffer.GetTime(col.BufferIndex);
                case DataType.DT_DBTIMESTAMP:
                    return buffer.GetDateTime(col.BufferIndex);
                case DataType.DT_DBTIMESTAMP2:
                    return buffer.GetDateTime(col.BufferIndex);
                case DataType.DT_DBTIMESTAMPOFFSET:
                    return buffer.GetDateTimeOffset(col.BufferIndex);
                case DataType.DT_DECIMAL:
                    return buffer.GetDecimal(col.BufferIndex);
                case DataType.DT_FILETIME:
                    return buffer.GetDateTime(col.BufferIndex);
                case DataType.DT_GUID:
                    return buffer.GetGuid(col.BufferIndex);
                case DataType.DT_I1:
                    return buffer.GetSByte(col.BufferIndex);
                case DataType.DT_I2:
                    return buffer.GetInt16(col.BufferIndex);
                case DataType.DT_I4:
                    return buffer.GetInt32(col.BufferIndex);
                case DataType.DT_I8:
                    return buffer.GetInt64(col.BufferIndex);
                case DataType.DT_IMAGE:
                    return buffer.GetBlobData(col.BufferIndex, 0, (int)buffer.GetBlobLength(col.BufferIndex));
                case DataType.DT_NTEXT:
                    return buffer.GetBlobData(col.BufferIndex, 0, (int)buffer.GetBlobLength(col.BufferIndex));
                case DataType.DT_NUMERIC:
                    return buffer.GetDecimal(col.BufferIndex);
                case DataType.DT_R4:
                    return buffer.GetSingle(col.BufferIndex);
                case DataType.DT_R8:
                    return buffer.GetDouble(col.BufferIndex);
                case DataType.DT_STR:
                    return buffer.GetString(col.BufferIndex);
                case DataType.DT_TEXT:
                    return buffer.GetBlobData(col.BufferIndex, 0, (int)buffer.GetBlobLength(col.BufferIndex));
                case DataType.DT_UI1:
                    return buffer.GetByte(col.BufferIndex);
                case DataType.DT_UI2:
                    return buffer.GetUInt16(col.BufferIndex);
                case DataType.DT_UI4:
                    return buffer.GetUInt32(col.BufferIndex);
                case DataType.DT_UI8:
                    return buffer.GetUInt64(col.BufferIndex);
                case DataType.DT_WSTR:
                    return buffer.GetString(col.BufferIndex);
                default:
                    return null;
            }
        }

        //Use user inputs to initalise run time variables
        private void StoreUserVariables()
        {
            //batchSize = Convert.ToInt32(base.ComponentMetaData.CustomPropertyCollection[BATCH_SIZE].Value.ToString());
            tableName = base.ComponentMetaData.CustomPropertyCollection[TABLE_NAME].Value.ToString();
            //currentRow = 0;
            //rowsObject = new object[colsCount, batchSize];
            //rowCount = 0;
            performAsTransaction = (bool)base.ComponentMetaData.CustomPropertyCollection[PERFORM_AS_TRANSACTION_NAME].Value;
            performPostgresCopy = (bool)base.ComponentMetaData.CustomPropertyCollection[PERFORM_POSTGRES_COPY].Value;
        }

        //Store all input column related data into the instantiated ColumnInfo class
        private void StoreInputColumns(IDTSInput100 input, int colsCount)
        {
            inputIndexes = new int[colsCount];
            columnInfos = new List<ColumnInfo>(this.ComponentMetaData.InputCollection[INPUT_NAME].InputColumnCollection.Count);
            for (int i = 0; i <= colsCount - 1; i++)
            {
                //Add each input column into an INT array                
                //Map all input and external input column data
                var col = input.InputColumnCollection[i];
                var externalColumnName = input.ExternalMetadataColumnCollection.GetObjectByID(input.InputColumnCollection[i].ExternalMetadataColumnID).Name;
                var externalSqlDbType = (NpgsqlDbType)input.ExternalMetadataColumnCollection.GetObjectByID(input.InputColumnCollection[i].ExternalMetadataColumnID).CustomPropertyCollection[POSTGRES_TYPE].Value;

                columnInfos.Add(new ColumnInfo(col.Name,
                                               col.ID,
                                               col.DataType,
                                               col.LineageID,
                                               this.BufferManager.FindColumnByLineageID(input.Buffer, col.LineageID),
                                               col.Length,
                                               col.Precision,
                                               col.Scale,
                                               externalColumnName,
                                               externalSqlDbType));
            }
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            try
            {
                if (!performPostgresCopy)
                {
                    // perform insert
                    while (buffer.NextRow())
                    {
                        // loop through column data in buffer and assign to parameters object
                        for (int i = 0; i <= colsCount - 1; i++)
                        {
                            //odpCmd.Parameters[":" + columnInfos[i].ExternalColumnName].Value = buffer[i];

                            // the order of columns in the buffer isn't always as expected.
                            // therefore, obtain the object/column value in the buffer using the column name.
                            object ob = GetBufferColumnValue(buffer, columnInfos[i]);

                            odpCmd.Parameters[":" + columnInfos[i].ExternalColumnName].Value = ob;
                        }

                        odpCmd.Prepare();
                        odpCmd.ExecuteNonQuery();
                    }
                }
                else
                {
                   
                    // perform postgres copy
                    using (var writer = odpConnection.BeginBinaryImport(odpCopyCommand))
                    {
                        while (buffer.NextRow())
                        {
                            writer.StartRow();

                            for (int i = 0; i <= colsCount - 1; i++)
                            {
                                // the order of columns in the buffer isn't always as expected.
                                // therefore, obtain the object/column value in the buffer using the column name.
                                object ob = GetBufferColumnValue(buffer, columnInfos[i]);

                                writer.Write(ob, columnInfos[i].ExternalSqlDbType);

                            }// end copy for
                        }//end copy while

                        writer.Complete();

                    }//end copy using 
                }//end copy

                if (performAsTransaction == true)
                {
                    if (!odpTran.IsCompleted)
                    {
                        odpTran.Commit();
                    }
                }
            }
            catch (Exception e)
            {
                FireEvent(EventType.Error, "Error in ProcessInput : " + e.StackTrace);
                if (performAsTransaction == true)
                {
                    if (!odpTran.IsCompleted)
                    {
                        odpTran.Rollback();
                    }
                }
                throw (e);
            }
        }

        #endregion

        #region Connection handling

        public override void AcquireConnections(object transaction)
        {

            if (!string.IsNullOrEmpty(this.ComponentMetaData.CustomPropertyCollection[TABLE_NAME].Value.ToString()))
            {
                try
                {
                    
                    odpConnection = (NpgsqlConnection)this.ComponentMetaData.RuntimeConnectionCollection[CONNECTION_NAME].ConnectionManager.AcquireConnection(transaction);

                    if (odpConnection.State == ConnectionState.Closed)
                    {

                        odpConnection.Open();
                    }
                }
                catch (Exception e)
                {
                    FireEvent(EventType.Error, "Error in AcquireConnections: " + e.StackTrace);

                    throw (e);
                }
            }
            base.AcquireConnections(transaction);
        }

        public override void ReleaseConnections()
        {
            base.ReleaseConnections();

            if (odpConnection != null)
            {
                if (odpConnection.State == ConnectionState.Open)
                {
                    odpConnection.Close();
                }
            }

            odpConnection = null;
        }
        #endregion

        #region DTS Error Handling

        enum EventType
        {
            Information = 0,
            Progress = 1,
            Warning = 2,
            Error = 3
        }
        private void FireEvent(EventType eventType, string description)
        {
            bool cancel = false;

            switch (eventType)
            {
                case EventType.Information:
                    this.ComponentMetaData.FireInformation(0, this.ComponentMetaData.Name, description, string.Empty, 0, ref cancel);
                    break;
                case EventType.Progress:
                    throw new NotImplementedException("Progress messages are not implemented");
                case EventType.Warning:
                    this.ComponentMetaData.FireWarning(0, this.ComponentMetaData.Name, description, string.Empty, 0);
                    break;
                case EventType.Error:
                    this.ComponentMetaData.FireError(0, this.ComponentMetaData.Name, description, string.Empty, 0, out cancel);
                    break;
                default:
                    this.ComponentMetaData.FireError(0, this.ComponentMetaData.Name, description, string.Empty, 0, out cancel);
                    break;
            }
        }

        public override DTSValidationStatus Validate()
        {
            try
            {
                var status = base.Validate();

                if (status != DTSValidationStatus.VS_ISVALID)
                {
                    return status;
                }

                if ((status = ValidateConnection()) != DTSValidationStatus.VS_ISVALID)
                    return status;

                if ((status = ValidateCustomProperties()) != DTSValidationStatus.VS_ISVALID)
                    return status;

                if ((status = ValidateInputs()) != DTSValidationStatus.VS_ISVALID)
                    return status;

                return status;

            }
            catch (Exception)
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }
        }

        private DTSValidationStatus ValidateConnection()
        {
            if (this.ComponentMetaData.RuntimeConnectionCollection[CONNECTION_NAME].ConnectionManager == null)
            {
                FireEvent(EventType.Error, "No connection manager");
                return DTSValidationStatus.VS_ISBROKEN;
            }

            //Property is valid
            return DTSValidationStatus.VS_ISVALID;
        }

        private DTSValidationStatus ValidateCustomProperties()
        {
            if (this.ComponentMetaData.CustomPropertyCollection[TABLE_NAME] == null)
            {
                FireEvent(EventType.Error, "No table name provided");
                return DTSValidationStatus.VS_ISBROKEN;
            }

            return DTSValidationStatus.VS_ISVALID;
        }

        //Validate Inputs  Both InputColumns and ExternalColumns
        private DTSValidationStatus ValidateInputs()
        {
            bool cancel;

            //Component should have a single input
            if (this.ComponentMetaData.InputCollection.Count != 1)
            {
                ErrorSupport.FireErrorWithArgs(HResults.DTS_E_INCORRECTEXACTNUMBEROFINPUTS, out cancel, 1);
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            //Check input has columns
            var input = ComponentMetaData.InputCollection[INPUT_NAME];
            if (input.ExternalMetadataColumnCollection.Count == 0)
            {
                return DTSValidationStatus.VS_NEEDSNEWMETADATA;
            }

            //Check input columns are valid
            if (!this.ComponentMetaData.AreInputColumnsValid)
            {
                input.InputColumnCollection.RemoveAll();
                return DTSValidationStatus.VS_NEEDSNEWMETADATA;
            }

            //Input truncation disposition not supported
            if (input.TruncationRowDisposition != DTSRowDisposition.RD_NotUsed)
            {
                ErrorSupport.FireError(HResults.DTS_E_ADODESTINPUTTRUNDISPNOTSUPPORTED, out cancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            return DTSValidationStatus.VS_ISVALID;
        }

        #endregion

    }

    internal class ColumnInfo
    {
        private string _columnName = string.Empty;
        private int _columnID = 0;
        private DataType _dataType = DataType.DT_STR;
        private int _lineageID = 0;
        private int _bufferIndex = 0;
        private int _precision = 0;
        private int _scale = 0;
        private int _length;
        private string _externalColumnName = string.Empty;
        private NpgsqlDbType _externalSqlDbType = NpgsqlDbType.Char;

        public ColumnInfo(string columnName, int columnID, DataType dataType, int lineageID, int bufferIndex, int length, int precision, int scale, string externalColumnName, NpgsqlDbType externalSqlDbType)
        {
            _columnName = columnName;
            _columnID = columnID;
            _dataType = dataType;
            _lineageID = lineageID;
            _bufferIndex = bufferIndex;
            _precision = precision;
            _scale = scale;
            _length = length;
            _externalColumnName = externalColumnName;
            _externalSqlDbType = externalSqlDbType;
        }

        public int BufferIndex
        { get { return _bufferIndex; } }

        public DataType ColumnDataType
        { get { return _dataType; } }

        public int LineageID
        { get { return _lineageID; } }

        public string ColumnName
        { get { return _columnName; } }

        public int ColumnID
        { get { return _columnID; } }

        public int Precision
        { get { return _precision; } }

        public int Length
        { get { return _length; } }

        public int Scale
        { get { return _scale; } }

        public string ExternalColumnName
        { get { return _externalColumnName; } }

        public NpgsqlDbType ExternalSqlDbType
        { get { return _externalSqlDbType; } }


    }
}
