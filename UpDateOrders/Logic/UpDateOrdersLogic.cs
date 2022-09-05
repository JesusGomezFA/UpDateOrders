using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
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
            UpdateOrdersSql(GenerateTableOracle(SetTableHistoricalSql()),GenerateDate());
            CreateFile(GenerateDate());
        }
        //obtiene la cantidad datos de la consulta realizada a SQL
        public static DataTable SetTableHistoricalSql()
        {
            using (SqlConnection conSql = new SqlConnection(GetConnectionSql()))
            {
                conSql.Open();
                Console.WriteLine("Obteniendo data Sql");
                //string query = "select * from pruebas where FECHA_CIERRE_ORDEN is null OR  FECHA_CIERRE_ORDEN = ''";
                string query = "select * from MM_DTMovistarP where FECHA_CIERRE_ORDEN is null OR  FECHA_CIERRE_ORDEN = ''";
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
        public static void UpdateOrdersSql(DataTable dataTableOracle, string archivo)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionSql()))
            {
                try
                {
                    Console.WriteLine("Inicio Envio de informacion BD");
                    connection.Open();
                    string dateArchivo = "UpdateOrders_" + archivo + ".csv";
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
                                sqlCommand.Parameters.AddWithValue("@archivo", dateArchivo.ToString());
                                sqlCommand.ExecuteNonQuery();
                            }

                        }
                    }
                    else
                    {
                        Console.WriteLine("No se hizo nada");
                    }
                }
                catch (Exception ex)
                {
                   
                    ErrorMessage(GenerateDate(),ex.Message);
                }
            }

        }
        //genera consulta y  creacion de archivo
        public static void CreateFile(string archivo)
        {
            using (SqlConnection sqlConnection = new SqlConnection(GetConnectionSql()))
            {
                sqlConnection.Open();
                int contador =0;
                SendFile sendFile = new SendFile();
                DateTime fecha = DateTime.Now;
                string fechaArchivo = archivo;
                string fecha_menos = fecha.AddDays(-1).ToString("d/MM/yyyy");
                //SqlDataAdapter AdapMV = new SqlDataAdapter("SELECT * FROM pruebas WHERE  FECHA_CIERRE_ORDEN LIKE '%" + fecha_menos + "%'", sqlConnection);
                SqlDataAdapter AdapMV = new SqlDataAdapter("SELECT * FROM MM_DTMovistarP WHERE  FECHA_CIERRE_ORDEN LIKE '%" + fecha_menos + "%'", sqlConnection);
                DataTable consulaData = new DataTable();
                try
                {
                    AdapMV.Fill(consulaData);
                }
                catch (Exception ex)
                {

                    Console.WriteLine("Error de Adaptador " + ex.Message);
                }
                try
                {
                    if (consulaData.Rows.Count>0)
                    {
                        //CREAMOS ARCHIVO CSV
                        StreamWriter sw = new StreamWriter(@"E:\Documentos\UpdateOrders_" + fechaArchivo + ".csv", false, Encoding.UTF8);
                        //StreamWriter sw = new StreamWriter(@"C:\Users\jsgomezpe2\Desktop\Trabajo Celula Axia\OneDrive - fractalia.es\archivos prueba\update\UpdateOrders_" + fechaArchivo + ".csv", false, Encoding.UTF8);
                        //copiar encabezados de la consulta
                        long cantidadColumnas = consulaData.Columns.Count;
                        for (int ncolumna = 0; ncolumna < cantidadColumnas; ncolumna++)
                        {
                            if (consulaData.Columns[ncolumna].ColumnName == "LOCALIDAD_CREACION_ALTA" || consulaData.Columns[ncolumna].ColumnName == "DESCRIPCION_ESTADO" || consulaData.Columns[ncolumna].ColumnName == "SKU" || consulaData.Columns[ncolumna].ColumnName == "NOMBRE_EQUIPO" || consulaData.Columns[ncolumna].ColumnName == "REFERENCIA_PAGO" || consulaData.Columns[ncolumna].ColumnName == "FECHA_PAGO" || consulaData.Columns[ncolumna].ColumnName == "VALOR_PAGO" || consulaData.Columns[ncolumna].ColumnName == "ARCHIVO" || consulaData.Columns[ncolumna].ColumnName == "FECHA_PROGRAMADA_ENTREGA")
                            {

                            }
                            else
                            {

                                sw.Write(consulaData.Columns[ncolumna]);
                                if (ncolumna < cantidadColumnas - 1)
                                {
                                    sw.Write("|");
                                }
                            }
                        }
                        sw.Write(sw.NewLine); //saltamos linea
                        foreach (DataRow renglon in consulaData.Rows)
                        {
                            for (int ncolumna = 0; ncolumna < cantidadColumnas; ncolumna++)
                            {
                                if (consulaData.Columns[ncolumna].ColumnName == "LOCALIDAD_CREACION_ALTA" || consulaData.Columns[ncolumna].ColumnName == "DESCRIPCION_ESTADO" || consulaData.Columns[ncolumna].ColumnName == "SKU" || consulaData.Columns[ncolumna].ColumnName == "NOMBRE_EQUIPO" || consulaData.Columns[ncolumna].ColumnName == "REFERENCIA_PAGO" || consulaData.Columns[ncolumna].ColumnName == "FECHA_PAGO" || consulaData.Columns[ncolumna].ColumnName == "VALOR_PAGO" || consulaData.Columns[ncolumna].ColumnName == "ARCHIVO" || consulaData.Columns[ncolumna].ColumnName == "FECHA_PROGRAMADA_ENTREGA")
                                {

                                }
                                else
                                {
                                    if (!Convert.IsDBNull(renglon[ncolumna]))
                                    {
                                        sw.Write(renglon[ncolumna]);
                                    }
                                    if (ncolumna < cantidadColumnas)
                                    {
                                        sw.Write("|");
                                    }
                                }
                                
                            }
                            sw.Write(sw.NewLine); //saltamos linea
                            contador++;
                        }
                        if (contador < consulaData.Rows.Count)
                        {
                            string error = "No se envian todos los datos en el archivos";
                            ErrorMessage(GenerateDate(), error);
                        }
                        sw.Close();
                        sqlConnection.Close();
                        sendFile.Send(@"E:\Documentos\UpdateOrders_" + fechaArchivo + ".csv");
                        //sendFile.Send(@"C:\Users\jsgomezpe2\Desktop\Trabajo Celula Axia\OneDrive - fractalia.es\archivos prueba\update\UpdateOrders_" + fechaArchivo + ".csv");
                        sqlConnection.Dispose();
                        
                    }
                    else
                    {
                        //CREAMOS ARCHIVO CSV
                        StreamWriter sw = new StreamWriter(@"E:\Documentos\UpdateOrders_" + fechaArchivo + ".csv", false, Encoding.UTF8);
                        sw.Write("no se encontraron registros");
                        sw.Write(sw.NewLine); //saltamos linea
                        sw.Close();
                        sqlConnection.Close();
                        sendFile.Send(@"E:\Documentos\UpdateOrders_" + fechaArchivo + ".csv");
                        sqlConnection.Dispose();
                    }
                }
                catch (Exception ex)
                {

                    if (sqlConnection.State == ConnectionState.Open)
                    {
                        ErrorMessage(GenerateDate(),ex.Message);
                    }
                    else
                    {
                        ErrorMessage(GenerateDate(), ex.Message);
                    }
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
        //Mensaje de error
        public static void ErrorMessage(string archivo, string error)
        {
            using (SqlConnection con = new SqlConnection(GetConnectionSql()))
            {
                con.Open();
                string FechaArchivo = DateTime.Now.ToString("dd-MM-yyyy");
                string horaArchivo = DateTime.Now.ToString("HH-mm");
                string insert = "insert into Errors(Fecha,Problema,Consola) values ('" + FechaArchivo + "_" + horaArchivo + "','Error_ "+error+"_al crear el archivo UpdateOrders_"+ archivo + ".csv','UpdateOrders')";
                SqlCommand comando = new SqlCommand(insert, con);
                comando.ExecuteNonQuery();
                con.Close();
            }
            Console.WriteLine("Error Enviado a DB");
        }
        //Genera la fecha con la cual se va a guardar el archivo
        public static string GenerateDate()
        {
            string fechaArchivo = DateTime.Now.ToString("dd-MM-yyyy");
            string horaArchivo = DateTime.Now.ToString("HH-mm");
            string archivo = ""+ fechaArchivo + "_" + horaArchivo + "";
            return archivo;
        }

    }
}
