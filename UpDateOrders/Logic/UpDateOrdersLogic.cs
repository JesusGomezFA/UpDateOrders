using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using UpDateOrders.Data;

namespace UpDateOrders.Logic
{
    internal class UpDateOrdersLogic
    {
        public static void UpDateOrder()
        {
            CargarArchivo();

        }

        public static void CargarArchivo()
        {
            UpdateOrdersSql(GenerateTableOracle(SetTableHistoricalSql()));
        }
        //obtiene la cantidad datos de la consulta realizada a SQL
        public static DataTable SetTableHistoricalSql()
        {
            using (SqlConnection conSql = new SqlConnection(GetConnectionSql()))
            {
                conSql.Open();
                Console.WriteLine("Obteniendo data Sql");
                string query = "select * from pruebas where FECHA_CIERRE_ORDEN is null OR  FECHA_CIERRE_ORDEN = ''";
                //string query = "select * from MM_DTMovistarP where FECHA_CIERRE_ORDEN is null OR  FECHA_CIERRE_ORDEN = ''";
                SqlDataAdapter Historico = new SqlDataAdapter(query, conSql);
                DataTable DbHistorical = new DataTable();
                Historico.Fill(DbHistorical);
                conSql.Close();
                return DbHistorical;
            }
        }
        //se ejecuta query de Oracle con los datos obtenidos de la consulta SQL
        public static DataTable GenerateTableOracle(DataTable DbHistorical)
        {
            using(SqlConnection connectioSql = new SqlConnection(GetConnectionSql()))
            {
                connectioSql.Open();
                using (OracleConnection conOracle = new OracleConnection(GetConnectionOracle()))
                {
                    conOracle.Open();
                    Console.WriteLine("Inicio Consulta Oracle");
                    List<string> listaConsulta = new List<string>();
                    string queryMV;
                    string union;
                    string query = "select * from MM_PGeneral where id = '2'";
                    SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(query,connectioSql);
                    DataTable consultaOracle = new DataTable();
                    consultaOracle = DbHistorical.Copy();
                    DataTable dbOracle = new DataTable();
                    DataTable dataTableOracle = new DataTable();
                    sqlDataAdapter.Fill(dbOracle);
                    for (int i =0; i<=consultaOracle.Rows.Count -1 ;i++)
                    {
                        listaConsulta.Add("'"+ consultaOracle.Rows[i]["OrderID"].ToString() +"'");
                        if (listaConsulta.Count == 999 || i ==consultaOracle.Rows.Count -1)
                        {
                            union = string.Join(",", listaConsulta);
                            queryMV = dbOracle.Rows[0]["Query"].ToString();
                            queryMV = queryMV.Replace("()", "(" + union + ")");
                            OracleCommand oracleCommand = new OracleCommand(queryMV, conOracle);
                            oracleCommand.CommandType = CommandType.Text;
                            OracleDataReader oracleDataReader = oracleCommand.ExecuteReader();
                            dataTableOracle.Load(oracleDataReader);
                            listaConsulta.Clear();
                            queryMV = "";

                        }
                    }
                    Console.WriteLine("Fin De consulta");
                    conOracle.Close();
                    return dataTableOracle;
                }
            }
        }
        //metodo para realizar conexion con base de datos de SQL
        public static void UpdateOrdersSql(DataTable dataTableOracle)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionSql()))
            {
                Console.WriteLine("Inicio Envio de informacion BD");
                connection.Open();
                string DateFile=;
                DataTable dataTableBulkcopy = new DataTable();
                dataTableBulkcopy = dataTableOracle.Copy();
                if (dataTableBulkcopy.Rows.Count > 0)
                {
                    Console.WriteLine("Update en base de datos");
                    for (int i = 0; i < dataTableBulkcopy.Rows.Count; i++)
                    {
                        SqlCommand sqlCommand = new SqlCommand("update_Prueba", connection);
                        if (!String.IsNullOrEmpty(dataTableBulkcopy.Rows[i]["FECHA_CIERRE_ORDEN"].ToString()))
                        {
                            sqlCommand.CommandType = CommandType.StoredProcedure;
                            sqlCommand.Parameters.AddWithValue("@orderID", dataTableBulkcopy.Rows[i]["ORDER_ID"].ToString());
                            sqlCommand.Parameters.AddWithValue("@fecha_creacion", dataTableBulkcopy.Rows[i]["FECHA_CREACION"].ToString().Length > 0 ? dataTableBulkcopy.Rows[i]["FECHA_CREACION"].ToString().Substring(0, 10) : "");
                            sqlCommand.Parameters.AddWithValue("@estado_orden", dataTableBulkcopy.Rows[i]["ESTADO_ORDEN"].ToString());
                            sqlCommand.Parameters.AddWithValue("@cliente", dataTableBulkcopy.Rows[i]["CLIENTE"].ToString());
                            sqlCommand.Parameters.AddWithValue("@cuenta", dataTableBulkcopy.Rows[i]["CUENTA"].ToString());
                            sqlCommand.Parameters.AddWithValue("@tipo_identificacion", dataTableBulkcopy.Rows[i]["TIPO_IDENTIFICACION"].ToString());
                            sqlCommand.Parameters.AddWithValue("@numero_identificacion", dataTableBulkcopy.Rows[i]["NUMERO_IDENTIFICACION"].ToString());
                            sqlCommand.Parameters.AddWithValue("@numero_celular", dataTableBulkcopy.Rows[i]["NUMERO_CELULAR"].ToString());
                            sqlCommand.Parameters.AddWithValue("@fecha_cierre_orden", dataTableBulkcopy.Rows[i]["FECHA_CIERRE_ORDEN"].ToString().Length > 0 ? dataTableBulkcopy.Rows[i]["FECHA_CIERRE_ORDEN"].ToString().Substring(0, 10) : "");
                            sqlCommand.Parameters.AddWithValue("@archivo", DateFile.ToString());
                            sqlCommand.ExecuteNonQuery();
                        }
                    }
                }
                else
                {
                    Console.WriteLine("No se hizo nada");
                }

            }

        }
        public static string GetConnectionSql()
        {
            Connection connection = new Connection();
            try
            {
                SqlConnection connectionSql = new SqlConnection();
                return connectionSql.ConnectionString = connection.GetConnectionMySql(); ;
            }
            catch (Exception ex)
            {

                Console.WriteLine("Error en conexion con SQL");
                SqlConnection connectionSql = new SqlConnection();
                connectionSql.Open();
                string insert = $"insert into MM_Log(Fecha,Problema,Consola) " +
                    $"values ('{FechaArchivo()}','problema al conectar con base de datos SQL','FileUpload')";
                SqlCommand comando = new SqlCommand(insert, connectionSql);
                comando.ExecuteNonQuery();
                connectionSql.Close();
                Console.WriteLine("error: " + ex.Message);
                return null;
            }
        }
        //metodo para realizar conexion con base de datos Oracle
        public static string GetConnectionOracle()
        {

            try
            {
                Connection connection = new Connection();
                OracleConnection connectionOracle = new OracleConnection();
                return connectionOracle.ConnectionString = connection.GetConnectionOracle(); ;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error en conexion con oracle");
                using (SqlConnection cn = new SqlConnection(GetConnectionSql()))
                {
                    cn.Open();
                    string insert = $"insert into MM_Log(Fecha,Problema,Consola) values ('{FechaArchivo()}','problema al conectar con base de datos ORACLE','FileUpload')";
                    SqlCommand comando = new SqlCommand(insert, cn);
                    comando.ExecuteNonQuery();
                    cn.Close();
                }
                Console.WriteLine("error: " + ex.Message);
                return null;

            }

        }
        // el metodo agrega la fecha actual con la hora para los mensajes de error
        public static string FechaArchivo()
        {
            DateTime fechaArchivo = DateTime.Now;
            string fecha_menos = fechaArchivo.AddDays(-1).ToString("dd/MM/yyyy");
            return fecha_menos;
        }
        public static void ErrorMessage()
        {
            using (SqlConnection con = new SqlConnection(GetConnectionSql()))
            {
                string FechaArchivo = DateTime.Now.ToString("dd-MM-yyyy");
                string horaArchivo = DateTime.Now.ToString("HH-mm");
                string insert = "insert into MM_Log(Fecha,Problema,Consola) values ('" + FechaArchivo + "_" + horaArchivo + "','Error al crear el archivo .csv','SuscriberEvents')";
                SqlCommand comando = new SqlCommand(insert, con);
            }
            Console.WriteLine("Error Enviado a DB");
        }
        //Genera la fecha con la cual se va a guardar el archivo
        public static string GenerateDate()
        {

            return null;
        }

    }
}
