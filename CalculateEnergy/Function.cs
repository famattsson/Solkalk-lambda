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
        }



        public void FunctionHandler(ILambdaContext context)
        {
            CalculateProducedPower();
        }

        static public Dictionary<string, string> GetMunicipalIrrradianceData()
        {
            Dictionary<string, Tuple<float, float>> kommunDict = new Dictionary<string, Tuple<float, float>>()
            {
                { "Aneby",Tuple.Create(57f, 15f) }, { "Tranås", Tuple.Create(58f,15f) },
                { "Nässjö",Tuple.Create(58f, 15f) }, { "Eksjö", Tuple.Create(57f,15f) },
                { "Vetlanda",Tuple.Create(57f, 15f) }, { "Sävsjö", Tuple.Create(57f,15f) },
                { "Värnamo",Tuple.Create(57f, 14f) }, { "Gislaved", Tuple.Create(57f, 14f) },
                { "Vaggeryd",Tuple.Create(58f, 14f) }, { "Jönköping", Tuple.Create(58f,14f) },
                { "Habo",Tuple.Create(58f, 14f) }, { "Mullsjö", Tuple.Create(58f,14f) },
                { "Gnosjö",Tuple.Create(57f, 14f) }
            };
            Dictionary<string, string> responseDict = new Dictionary<string, string>();
            var date = DateTime.Today;
            var hour = 0;
            var day = date.AddDays(-1).Day;
            var year = date.Year;
            var month = date.Month;
            var nextDay = date.AddDays(1).Day;
            foreach (var elem in kommunDict)
            {
                var requestUrl = String.Format("http://strang.smhi.se/extraction/getseries.php?par=117&m1={0}&d1={1}&y1={2}&h1={3}&m2={4}&d2={5}&y2={6}&h2={7}&lat={8}&lon={9}&lev=0",
                month, day, year, hour, month, nextDay, year, hour, elem.Value.Item1, elem.Value.Item2);

                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(requestUrl);
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                Stream responseStream = response.GetResponseStream();
                StreamReader streamReader = new StreamReader(responseStream, Encoding.UTF8);
                string responseBody = streamReader.ReadToEnd();
                responseDict[elem.Key] = responseBody;
            }
            return responseDict;
        }

        static public string GetMunicipalInstalledPowerData()
        {
            string data = @"Aneby 299000
Tranås 530000
Nässjö 1377000
Eksjö 611000
Vetlanda 1484000
Sävsjö 680000
Värnamo 1863000
Gislaved 1123000
Gnosjö 243000
Vaggeryd 419000
Jönköping 3983000
Habo 206000
Mullsjö 367000";
            return data;
        }

        static public List<PowerRecord> GetMunicipalIrradianceRecords()
        {
            Dictionary<string, string> irradianceData = GetMunicipalIrrradianceData();
            List<PowerRecord> irradianceRecords = new List<PowerRecord>();
            foreach (var elem in irradianceData)
            {
                string[] dataLines = elem.Value.Split('\n');
                foreach (var dataLine in dataLines)
                {
                    if (dataLine != "")
                    {
                        PowerRecord irradianceRecord = new PowerRecord();
                        irradianceRecord.Name = elem.Key;
                        string[] data = dataLine.Split(' ');
                        irradianceRecord.date.year = data[0];
                        irradianceRecord.date.month = data[1];
                        irradianceRecord.date.day = data[2];
                        irradianceRecord.date.hour = data[3];
                        irradianceRecord.Energy = float.Parse(data[4], CultureInfo.InvariantCulture.NumberFormat);
                        irradianceRecords.Add(irradianceRecord);
                    }
                }
            }
            return irradianceRecords;
        }

        static public Dictionary<string, float> GetMunicipalInstalledPowerDict()
        {
            Dictionary<string, float> dataDict = new Dictionary<string, float>();
            string data = GetMunicipalInstalledPowerData();
            string[] dataLines = data.Split('\r', '\n');
            foreach (var elem in dataLines)
            {
                if (elem != "" && elem != "\r")
                {
                    string[] dataLine = elem.Split(' ');
                    dataDict[dataLine[0]] = Int32.Parse(dataLine[1]);
                }

            }
            return dataDict;
        }
        static public Dictionary<string, List<string>> GetCompanyIrrradianceData()
        {
            Dictionary<string, Tuple<float, float>[]> companyDict = new Dictionary<string, Tuple<float, float>[]>
            {
                { "Husqvarna", new[] { Tuple.Create(57f, 15f)} }
            };
            Dictionary<string, List<string>> responseDict = new Dictionary<string, List<string>>();
            var date = DateTime.Today;
            var hour = 0;
            var day = date.AddDays(-1).Day;
            var year = date.Year;
            var month = date.Month;
            var nextDay = date.AddDays(1).Day;
            foreach (var company in companyDict)
            {
                List<string> responses = new List<string>();
                foreach (var location in company.Value)
                {
                    var requestUrl = String.Format("http://strang.smhi.se/extraction/getseries.php?par=117&m1={0}&d1={1}&y1={2}&h1={3}&m2={4}&d2={5}&y2={6}&h2={7}&lat={8}&lon={9}&lev=0",
                    month, day, year, hour, month, nextDay, year, hour, location.Item1, location.Item2);

                    HttpWebRequest request = (HttpWebRequest)WebRequest.Create(requestUrl);
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                    Stream responseStream = response.GetResponseStream();
                    StreamReader streamReader = new StreamReader(responseStream, Encoding.UTF8);
                    string responseBody = streamReader.ReadToEnd();
                    responses.Add(responseBody);
                    responseDict[company.Key] = responses;
                }
            }
            return responseDict;
        }

        static public string GetCompanyInstalledPowerData()
        {
            string data = @"Husqvarna 17500";
            return data;
        }

        static public List<PowerRecord> GetCompanyIrradianceRecords()
        {
            Dictionary<string, List<string>> irradianceData = GetCompanyIrrradianceData();
            List<PowerRecord> irradianceRecords = new List<PowerRecord>();
            foreach (var company in irradianceData)
            {
                foreach (var location in company.Value)
                {
                    string[] dataLines = location.Split('\n');
                    foreach (var dataLine in dataLines)
                    {
                        if (dataLine != "")
                        {
                            PowerRecord irradianceRecord = new PowerRecord();
                            irradianceRecord.Name = company.Key;
                            string[] data = dataLine.Split(' ');
                            irradianceRecord.date.year = data[0];
                            irradianceRecord.date.month = data[1];
                            irradianceRecord.date.day = data[2];
                            irradianceRecord.date.hour = data[3];
                            irradianceRecord.Energy = float.Parse(data[4], CultureInfo.InvariantCulture.NumberFormat);
                            irradianceRecords.Add(irradianceRecord);
                        }
                    }
                }
            }
            return irradianceRecords;
        }

        static public Dictionary<string, float> GetCompanyInstalledPowerDict()
        {
            Dictionary<string, float> dataDict = new Dictionary<string, float>();
            string data = GetCompanyInstalledPowerData();
            string[] dataLines = data.Split('\r', '\n');
            foreach (var elem in dataLines)
            {
                if (elem != "" && elem != "\r")
                {
                    string[] dataLine = elem.Split(' ');
                    dataDict[dataLine[0]] = Int32.Parse(dataLine[1]);
                }

            }
            return dataDict;
        }

        static public void CalculateProducedPower()
        {
            Dictionary<string, Dictionary<Date, double>> munDict =
                new Dictionary<string, Dictionary<Date, double>>();

            Dictionary<string, float> installedPowerDict = GetMunicipalInstalledPowerDict();
            List<PowerRecord> irradianceRecords = GetMunicipalIrradianceRecords();

            foreach (var municipality in installedPowerDict)
            {
                Dictionary<Date, double> powerDict = new Dictionary<Date, double>();
                foreach (var irradiance in irradianceRecords)
                {
                    var area = municipality.Value / (0.15 * 1000); //Kommunal installerad effekt/(verkningsgrad * Irradians vid STC)
                    powerDict[irradiance.date] = irradiance.Energy * 0.15 * 0.9 * area;
                }
                munDict[municipality.Key] = powerDict;
            }
            AddMunicipalPowerRecord(munDict);

            Dictionary<string, Dictionary<Date, double>> companyDict =
                new Dictionary<string, Dictionary<Date, double>>();

            List<PowerRecord> companyRecords = GetCompanyIrradianceRecords();
            Dictionary<string, float> installedCompanyPower = GetCompanyInstalledPowerDict();

            foreach (var company in installedCompanyPower)
            {
                Dictionary<Date, double> powerDict = new Dictionary<Date, double>();
                foreach (var irradiance in irradianceRecords)
                {
                    var area = company.Value / (0.15 * 1000); //Kommunal installerad effekt/(verkningsgrad * Irradians vid STC)
                    powerDict[irradiance.date] = irradiance.Energy * 0.15 * 0.9 * area;
                }
                companyDict[company.Key] = powerDict;
            }
            AddCompanyPowerRecord(companyDict);
        }

        static public void AddMunicipalPowerRecord(Dictionary<string, Dictionary<Date, double>> powerDict)
        {
            foreach (var elem in powerDict)
            {
                var powerRecord = new PowerRecord();
                powerRecord.Name = elem.Key;
                var lastpowerrecord = powerRecord;

                foreach (var elem2 in elem.Value)
                {
                    powerRecord.date.year = elem2.Key.year;
                    powerRecord.date.month = elem2.Key.month;
                    powerRecord.date.day = elem2.Key.day;
                    powerRecord.Energy += elem2.Value;

                }
                using (var conn = new SqlConnection("Data Source=solkalkdb.cqjgliexpw2a.eu-west-1.rds.amazonaws.com;Initial Catalog=SolkalkDb;Integrated Security=False;User ID=NFK2018;Password=NFKsolkalk;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
                {
                    var query = "INSERT INTO ProducedMunicipalPower VALUES(@kommun,@energi,@year,@month,@day,@hour)";
                    using (var command = new SqlCommand(query, conn))
                    {
                        command.Parameters.Add("@energi", SqlDbType.Float).Value = powerRecord.Energy;
                        command.Parameters.Add("@kommun", SqlDbType.VarChar).Value = powerRecord.Name;
                        command.Parameters.Add("@year", SqlDbType.VarChar).Value = powerRecord.date.year;
                        command.Parameters.Add("@month", SqlDbType.VarChar).Value = powerRecord.date.month;
                        command.Parameters.Add("@day", SqlDbType.VarChar).Value = powerRecord.date.day;
                        command.Parameters.Add("@hour", SqlDbType.VarChar).Value = "00";
                        command.Connection.Open();
                        command.ExecuteNonQuery();
                        command.Connection.Close();
                    }
                }
            }
        }
        static public void AddCompanyPowerRecord(Dictionary<string, Dictionary<Date, double>> powerDict)
        {
            foreach (var elem in powerDict)
            {
                var powerRecord = new PowerRecord();
                powerRecord.Name = elem.Key;
                var lastpowerrecord = powerRecord;

                foreach (var elem2 in elem.Value)
                {
                    powerRecord.date.year = elem2.Key.year;
                    powerRecord.date.month = elem2.Key.month;
                    powerRecord.date.day = elem2.Key.day;
                    powerRecord.Energy += elem2.Value;

                }
                using (var conn = new SqlConnection("Data Source=solkalkdb.cqjgliexpw2a.eu-west-1.rds.amazonaws.com;Initial Catalog=SolkalkDb;Integrated Security=False;User ID=NFK2018;Password=NFKsolkalk;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
                {
                    var query = "INSERT INTO ProducedCompanyPower VALUES(@företag,@energi,@year,@month,@day,@hour)";
                    using (var command = new SqlCommand(query, conn))
                    {
                        command.Parameters.Add("@energi", SqlDbType.Float).Value = powerRecord.Energy;
                        command.Parameters.Add("@företag", SqlDbType.VarChar).Value = powerRecord.Name;
                        command.Parameters.Add("@year", SqlDbType.VarChar).Value = powerRecord.date.year;
                        command.Parameters.Add("@month", SqlDbType.VarChar).Value = powerRecord.date.month;
                        command.Parameters.Add("@day", SqlDbType.VarChar).Value = powerRecord.date.day;
                        command.Parameters.Add("@hour", SqlDbType.VarChar).Value = "00";
                        command.Connection.Open();
                        command.ExecuteNonQuery();
                        command.Connection.Close();
                    }
                }
            }
        }
    }
}
