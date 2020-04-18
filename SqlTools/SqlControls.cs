using SqlTools.Helpers;
using SqlTools.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlTools
{
    public class SqlControls : IDisposable
    {

        private SqlDataAdapter _sqlDataAdapter;


        private readonly string _connectionString;

        public SqlControls(ConnectionProfile sqlConnection)
        {
          this._connectionString = sqlConnection.GeneratedConstring;
        }

        public void UpdateRange<T>(string table, List<T> entity) where T : class
        {
            var tempTableName = "Temp";
            var con = new SqlConnection(_connectionString);
            try
            {
                if (con.State != ConnectionState.Open)
                    con.Open();

                var sqlCommand = new SqlCommand("", con);

                const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic;
                var objFieldNames = (from PropertyInfo aProp in typeof(T).GetProperties(flags)
                                     select new
                                     {
                                         Name = aProp.Name,
                                         Type = Nullable.GetUnderlyingType(aProp.PropertyType) ?? aProp.PropertyType
                                     }).ToList();

                var columns = "";

                for (var index = 0; index < objFieldNames.Count; index++)
                {
                    var fieldName = objFieldNames[index];
                    columns += index == objFieldNames.Count - 1
                       ? $"{fieldName.Name} {DetermineDbColumn(fieldName.Type)}"
                       : $"{fieldName.Name} {DetermineDbColumn(fieldName.Type)},";
                }
                sqlCommand.CommandText = $"Create Table #{tempTableName} ({columns})";
                sqlCommand.ExecuteNonQuery();

                using (var bulkcopy = new SqlBulkCopy(con))
                {
                    bulkcopy.BulkCopyTimeout = 660;
                    bulkcopy.DestinationTableName = $"#{tempTableName}";
                    bulkcopy.WriteToServer(ConverterTool.ToDataTable(entity));
                    bulkcopy.Close();
                }
                columns = "";
                for (var index = 1; index < objFieldNames.Count; index++)
                {
                    var field = objFieldNames[index];
                    columns += index == objFieldNames.Count - 1
                       ? $"{field.Name} = E.{field.Name}"
                       : $"{field.Name} = E.{field.Name},";
                }
                sqlCommand.CommandTimeout = 300;
                sqlCommand.CommandText =
                   $"Update {table} Set {columns} From {table} T INNER Join #{tempTableName} E ON T.Id = E.Id";
                sqlCommand.ExecuteNonQuery();
            }
            catch (Exception exception)
            {
                throw new Exception(exception.ToString());
            }
            finally
            {
                con.Close();
            }

        }
        private void PutValueToParameter(SqlCommand cmd, string paramsname, object value)
        {
            switch (Type.GetTypeCode(value.GetType()))
            {
                case TypeCode.Boolean:
                    cmd.Parameters[paramsname].SqlDbType = SqlDbType.Bit;
                    cmd.Parameters[paramsname].Value = Convert.ToBoolean(value);
                    break;
                case TypeCode.Int32:
                    cmd.Parameters[paramsname].SqlDbType = SqlDbType.Int;
                    cmd.Parameters[paramsname].Value = Convert.ToInt32(value);
                    break;
                case TypeCode.Double:
                    cmd.Parameters[paramsname].SqlDbType = SqlDbType.Decimal;
                    cmd.Parameters[paramsname].Value = Convert.ToDecimal(value);
                    break;
                case TypeCode.DateTime:
                    cmd.Parameters[paramsname].SqlDbType = SqlDbType.DateTime;
                    cmd.Parameters[paramsname].Value = Convert.ToDateTime(value);
                    break;
                case TypeCode.String:
                    cmd.Parameters[paramsname].SqlDbType = SqlDbType.VarChar;
                    cmd.Parameters[paramsname].Value = value.ToString();
                    break;
            }
        }

        private string DetermineDbColumn(Type value)
        {
            var type = "";
            switch (Type.GetTypeCode(value))
            {
                case TypeCode.Int32:
                    type = "Int";
                    break;
                case TypeCode.Boolean:
                    type = "Bit";
                    break;
                case TypeCode.String:
                    type = "VarChar(255)";
                    break;
                case TypeCode.DateTime:
                    type = "DateTime";
                    break;
            }
            return type;
        }

        private void PutValueToParameter(SqlCommand cmd, string paramsname, DataTable datalistBulk)
        {
            cmd.Parameters.Add(paramsname, SqlDbType.Structured).Value = datalistBulk;
        }
        private void AddParamaterWithValue(SqlCommand cmd, string paramsname, object value)
        {
            switch (Type.GetTypeCode(value.GetType()))
            {
                case TypeCode.Boolean:
                    cmd.Parameters.Add(paramsname, SqlDbType.Bit).Value = Convert.ToBoolean(value);
                    break;
                case TypeCode.Int32:
                    cmd.Parameters.Add(paramsname, SqlDbType.Int).Value = Convert.ToInt32(value);
                    break;
                case TypeCode.Double:
                    cmd.Parameters.Add(paramsname, SqlDbType.Decimal).Value = Convert.ToDouble(value);
                    break;
                case TypeCode.DateTime:
                    cmd.Parameters.Add(paramsname, SqlDbType.DateTime).Value = Convert.ToDateTime(value);
                    break;
                case TypeCode.String:
                    cmd.Parameters.Add(paramsname, SqlDbType.VarChar).Value = value.ToString();
                    break;
            }
        }
        public List<TSource> MultiSelectData<TSource>(string storedProcedure, params object[] parameterValue)
            where TSource : new()
        {
            var con = new SqlConnection(_connectionString);
            try
            {
                SqlCommand cmd = new SqlCommand(storedProcedure, con);
                for (var i = 1; i <= 50; i++)
                {
                    try
                    {
                        if (con.State != ConnectionState.Open)
                            con.Open();

                        cmd.CommandType = CommandType.StoredProcedure;

                        SqlCommandBuilder.DeriveParameters(cmd);
                        int cache = cmd.Parameters.Count;
                        for (var index = 1; index < cache; index++)
                        {
                            var paramaterName = cmd.Parameters[index].ParameterName;
                            var paramaterValue = parameterValue[index - 1];
                            PutValueToParameter(cmd, paramaterName, paramaterValue);
                        }

                        cmd.ExecuteNonQuery();

                        var dataSet = new DataSet();
                        _sqlDataAdapter = new SqlDataAdapter(cmd);
                        _sqlDataAdapter.Fill(dataSet);
                        dataSet.Dispose();

                        var dataList = new List<TSource>();

                        const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic;
                        var objFieldNames = (from PropertyInfo aProp in typeof(TSource).GetProperties(flags)
                                             select new { Name = aProp.Name, Type = Nullable.GetUnderlyingType(aProp.PropertyType) ?? aProp.PropertyType }).ToList();
                        var dataTblFieldNames = (from DataColumn aHeader in dataSet.Tables[0].Columns
                                                 select new { Name = aHeader.ColumnName, Type = aHeader.DataType }).ToList();

                        var commonFields = objFieldNames.Intersect(dataTblFieldNames).ToList();

                        foreach (DataRow dataRow in dataSet.Tables[0].Rows)
                        {
                            var aTSource = new TSource();
                            foreach (var aField in commonFields)
                            {

                                PropertyInfo propertyInfos = aTSource.GetType().GetProperty(aField.Name);
                                var value = (dataRow[aField.Name] == DBNull.Value) ? null : dataRow[aField.Name]; //if database field is nullable
                                propertyInfos.SetValue(aTSource, value, null);
                            }
                            dataList.Add(aTSource);
                        }

                        return dataList;
                    }
                    catch (Exception exception)
                    {
                        Debug.Write(exception.Message);
                    }
                }
            }
            catch (SqlException ex)
            {
                throw new Exception("In Reading Data From DB ", ex);
            }
            finally
            {
                con.Close();
            }
            return new List<TSource>();
        }

        public TSource SingleSelect<TSource>(string storedProcedure, params object[] parameterValue)
          where TSource : new()
        {
            var con = new SqlConnection(_connectionString);
            try
            {
                SqlCommand cmd = new SqlCommand(storedProcedure, con);
                for (var i = 1; i <= 50; i++)
                {
                    try
                    {
                        if (con.State != ConnectionState.Open)
                            con.Open();
                        cmd.CommandType = CommandType.StoredProcedure;
                        SqlCommandBuilder.DeriveParameters(cmd);
                        int cache = cmd.Parameters.Count;
                        for (var index = 1; index < cache; index++)
                        {
                            var paramaterName = cmd.Parameters[index].ParameterName;
                            var paramaterValue = parameterValue[index - 1];
                            PutValueToParameter(cmd, paramaterName, paramaterValue);
                        }
                        cmd.ExecuteNonQuery();

                        var d = new DataSet();
                        _sqlDataAdapter = new SqlDataAdapter(cmd);
                        _sqlDataAdapter.Fill(d);
                        d.Dispose();

                        var data = new TSource();

                        const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic;
                        var objFieldNames = (from PropertyInfo aProp in typeof(TSource).GetProperties(flags)
                                             select new { Name = aProp.Name, Type = Nullable.GetUnderlyingType(aProp.PropertyType) ?? aProp.PropertyType }).ToList();
                        var dataTblFieldNames = (from DataColumn aHeader in d.Tables[0].Columns
                                                 select new { Name = aHeader.ColumnName, Type = aHeader.DataType }).ToList();
                        var commonFields = objFieldNames.Intersect(dataTblFieldNames).ToList();


                        foreach (var field in commonFields)
                        {
                            PropertyInfo propertyInfos = data.GetType().GetProperty(field.Name);
                            var value = (d.Tables[0].Rows[0][field.Name] == DBNull.Value)
                                ? null
                                : d.Tables[0].Rows[0][field.Name]; //if database field is nullable
                            propertyInfos.SetValue(data, value, null);

                        }
                        return data;
                    }
                    catch (Exception exception)
                    {
                        Debug.Write(exception.Message);
                    }

                }
            }
            catch (SqlException ex)
            {
                throw new Exception("In Reading Data From DB ", ex);
            }
            finally
            {
                con.Close();
            }
            return new TSource();
        }

        public void Add<T>(string table, T entity) where T : class
        {
            try
            {
                using (var con = new SqlConnection(_connectionString))
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();
                    var dataTable = ConverterTool.ToDataTable(entity);
                    using (var sqlBulk = new SqlBulkCopy(con))
                    {
                        sqlBulk.DestinationTableName = table;
                        foreach (DataColumn dataColumn in dataTable.Columns)
                            sqlBulk.ColumnMappings.Add(dataColumn.ColumnName, dataColumn.ColumnName);
                        sqlBulk.WriteToServer(dataTable);
                    }
                }            }
            catch (Exception e)
            {
                throw new Exception("Error is: " + e);
            }
        }

        public void AddRange<T>(string table, List<T> entity) where T : class
        {
            try
            {
                using (var con = new SqlConnection(_connectionString))
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();
                    var dataTable = ConverterTool.ToDataTable(entity);
                    using (var sqlBulk = new SqlBulkCopy(con))
                    {
                        sqlBulk.DestinationTableName = table;
                        foreach (DataColumn dataColumn in dataTable.Columns)
                            sqlBulk.ColumnMappings.Add(dataColumn.ColumnName, dataColumn.ColumnName);
                        sqlBulk.WriteToServer(dataTable);
                    }
                }
                
            }
            catch (Exception e)
            {
                throw new Exception("Error is: " + e);
            }
        }

        public void Dispose()
        {
            //con?.Dispose();
            _sqlDataAdapter?.Dispose();
        }
    }
}
