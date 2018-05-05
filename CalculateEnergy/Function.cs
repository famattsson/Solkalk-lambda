using System;
using System.Collections.Generic;
using System.Net;
using Amazon.Lambda.Core;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Data;
using System.Text;
using System.IO;
using System.Globalization;



// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]



namespace CalculateEnergy
{

    public class CalculateEnergy
    {

        public static List<string> company = new List<string>() { "C", "c", "company", "Company" };
        public static List<string> municipality = new List<string>() { "M", "m", "Municipality", "municipality" };

        public struct Date
        {
            public string year;
            public string month;
            public string day;
            public string hour;
            public string date;
        }

        public struct PowerRecord
        {
            public string Name;
            public Date date;
            public double Energy;
            public double AverageIrradiance;
            public int Inhabitants;
        }



        public void FunctionHandler(ILambdaContext context)
        {

            CalculateProducedPower("m");
            CalculateProducedPower("c");
        }


        static public Dictionary<string, List<string>> GetIrrradianceData(string type)
        {
            Dictionary<string, Tuple<float, float>[]> coordinateDict = new Dictionary<string, Tuple<float, float>[]>();
            if (company.Contains(type))
            {
                coordinateDict = new Dictionary<string, Tuple<float, float>[]>
                {
                    { "Husqvarna", new[] { Tuple.Create(57f, 15f)} }
                };
            }
            else if (municipality.Contains(type))
            {
                coordinateDict = new Dictionary<string, Tuple<float, float>[]>()
                {
                    { "Aneby", new[] { Tuple.Create(57f, 15f) } }, { "Tranås", new[] { Tuple.Create(58f,15f) } },
                    { "Nässjö", new[] { Tuple.Create(58f, 15f) } }, { "Eksjö", new[] { Tuple.Create(57f,15f) } },
                    { "Vetlanda", new[] {Tuple.Create(57f, 15f) } }, { "Sävsjö", new[] { Tuple.Create(57f,15f) } },
                    { "Värnamo", new[] { Tuple.Create(57f, 14f) } }, { "Gislaved", new[] { Tuple.Create(57f, 14f) } },
                    { "Vaggeryd", new[] { Tuple.Create(58f, 14f) } }, { "Jönköping", new[] { Tuple.Create(58f,14f) } },
                    { "Habo", new[] { Tuple.Create(58f, 14f) } }, { "Mullsjö", new[] { Tuple.Create(58f,14f) } },
                    { "Gnosjö", new[] {Tuple.Create(57f, 14f) } }
                };
            }

            Dictionary<string, List<string>> responseDict = new Dictionary<string, List<string>>();
            var date = DateTime.Today;
            var hour = 0;
            var day = date.AddDays(-1).Day;
            var year = date.Year;

            var month2 = date.Month;
            var month1 = month2;
            if (date.Day == 1)
                month1 = month2 - 1;
            var nextDay = date.Day;
            foreach (var coordinate in coordinateDict)
            {
                List<string> responses = new List<string>();
                foreach (var location in coordinate.Value)
                {
                    var requestUrl = String.Format("http://strang.smhi.se/extraction/getseries.php?par=117&m1={0}&d1={1}&y1={2}&h1={3}&m2={4}&d2={5}&y2={6}&h2={7}&lat={8}&lon={9}&lev=0",
                    month1, day, year, hour, month2, nextDay, year, hour, location.Item1, location.Item2);

                    HttpWebRequest request = (HttpWebRequest)WebRequest.Create(requestUrl);
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                    Stream responseStream = response.GetResponseStream();
                    StreamReader streamReader = new StreamReader(responseStream, Encoding.UTF8);
                    string responseBody = streamReader.ReadToEnd();
                    responses.Add(responseBody);
                    responseDict[coordinate.Key] = responses;
                }
            }
            return responseDict;
        }

        static public string GetInstalledPowerData(string type)
        {
            if (company.Contains(type))
            {
                string data = Properties.Resources.CompanyInstalledPowerData;
                return data;
            }
            else if (municipality.Contains(type))
            {
                string data = Properties.Resources.MunicipalInstalledPowerData;
                return data;
            }
            else
            {
                return null;
            }
        }

        static public Dictionary<string, List<PowerRecord>> GetIrradianceRecords(string type)
        {
            Dictionary<string, List<string>> irradianceData = GetIrrradianceData(type);
            Dictionary<string, List<PowerRecord>> irradianceRecords = new Dictionary<string, List<PowerRecord>>();
            foreach (var organization in irradianceData)
            {
                foreach (var location in organization.Value)
                {
                    irradianceRecords.Add(organization.Key, new List<PowerRecord>());
                    string[] dataLines = location.Split('\n');
                    foreach (var dataLine in dataLines)
                    {
                        if (dataLine != "")
                        {
                            PowerRecord irradianceRecord = new PowerRecord();
                            irradianceRecord.Name = organization.Key;
                            string[] data = dataLine.Split(' ');
                            irradianceRecord.date.year = data[0];
                            irradianceRecord.date.month = data[1];
                            irradianceRecord.date.day = data[2];
                            irradianceRecord.date.hour = data[3];
                            irradianceRecord.Energy = float.Parse(data[4], CultureInfo.InvariantCulture.NumberFormat);
                            irradianceRecords[organization.Key].Add(irradianceRecord);
                        }
                    }
                }
            }
            return irradianceRecords;
        }

        static public List<PowerRecord> GetInstalledPowerRecords(string type)
        {
            List<PowerRecord> installedPowerRecords = new List<PowerRecord>();
            string data = GetInstalledPowerData(type);
            string[] dataLines = data.Split('\r', '\n');
            foreach (var elem in dataLines)
            {
                if (elem != "" && elem != "\r")
                {
                    PowerRecord installedPowerRecord = new PowerRecord();
                    string[] dataLine = elem.Split(' ');
                    installedPowerRecord.Name = dataLine[0];
                    installedPowerRecord.Energy = Int32.Parse(dataLine[1]);
                    if (municipality.Contains(type))
                    {
                        installedPowerRecord.Inhabitants = Int32.Parse(dataLine[2]);
                    }
                    installedPowerRecords.Add(installedPowerRecord);

                }

            }
            return installedPowerRecords;
        }

        static public void CalculateProducedPower(string type)
        {
            List<PowerRecord> installedPowerRecords = GetInstalledPowerRecords(type);
            Dictionary<string, List<PowerRecord>> irradianceRecords = GetIrradianceRecords(type);

            List<PowerRecord> powerRecords = new List<PowerRecord>();
            foreach (var record in installedPowerRecords)
            {
                double totalIrradiance = 0;
                PowerRecord powerRecord = new PowerRecord();
                powerRecord.Name = record.Name;
                powerRecord.Inhabitants = record.Inhabitants;
                foreach (var irradiance in irradianceRecords[record.Name])
                {
                    var area = record.Energy / (0.15 * 1000); //Kommunal installerad effekt/(verkningsgrad * Irradians vid STC)

                    powerRecord.Energy += irradiance.Energy * 0.15 * 0.9 * area;
                    powerRecord.date = irradiance.date;
                    totalIrradiance += irradiance.Energy;
                }
                powerRecord.AverageIrradiance = totalIrradiance / 24;
                powerRecords.Add(powerRecord);
            }
            AddPowerRecords(powerRecords, type);
        }

        static public void AddPowerRecords(List<PowerRecord> powerRecords, string type)
        {
            var query = "";

            if (company.Contains(type))
            {
                query = "INSERT INTO ProducedCompanyPower VALUES(@kommun,@energi,@year,@month,@day,@irradiance)";
            }

            else if (municipality.Contains(type))
            {
                query = "INSERT INTO ProducedMunicipalPower VALUES(@kommun,@energi,@year,@month,@day,@inhabitants,@irradiance)";
            }

            foreach (var powerRecord in powerRecords)
            {
                using (var conn = new SqlConnection("Data Source=solkalkdb.cqjgliexpw2a.eu-west-1.rds.amazonaws.com;Initial Catalog=SolkalkDb;Integrated Security=False;User ID=NFK2018;Password=NFKsolkalk;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
                {
                    using (var command = new SqlCommand(query, conn))
                    {
                        command.Parameters.Add("@energi", SqlDbType.Float).Value = powerRecord.Energy;
                        command.Parameters.Add("@kommun", SqlDbType.VarChar).Value = powerRecord.Name;
                        command.Parameters.Add("@year", SqlDbType.VarChar).Value = powerRecord.date.year;
                        command.Parameters.Add("@month", SqlDbType.VarChar).Value = powerRecord.date.month;
                        command.Parameters.Add("@day", SqlDbType.VarChar).Value = powerRecord.date.day;
                        if (municipality.Contains(type))
                        {
                            command.Parameters.Add("@inhabitants", SqlDbType.Float).Value = powerRecord.date.day;
                        }
                        command.Parameters.Add("@irradiance", SqlDbType.Float).Value = powerRecord.AverageIrradiance;
                        command.Connection.Open();
                        command.ExecuteNonQuery();
                        command.Connection.Close();
                    }
                }
            }
        }
    }
}