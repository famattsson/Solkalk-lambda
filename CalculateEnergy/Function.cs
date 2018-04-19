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
            public string Kommun;
            public Date date;
            public double Energi;
        }



        public void FunctionHandler(ILambdaContext context)
        {
            CalculateProducedPower();
        }

        static public Dictionary<string, string> GetIrrradianceData()
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

        static public string GetInstalledPowerData()
        {
            string data = @"Aneby 249000
Tranås 527000
Nässjö 1020000
Eksjö 532000
Vetlanda 1320000
Sävsjö 591000
Värnamo 1788000
Gislaved 1080000
Gnosjö 243000
Vaggeryd 406000
Jönköping 3952000
Habo 176000
Mullsjö 358000";
            return data;
        }

        static public List<PowerRecord> GetIrradianceRecords()
        {
            Dictionary<string, string> irradianceData = GetIrrradianceData();
            List<PowerRecord> irradianceRecords = new List<PowerRecord>();
            foreach (var elem in irradianceData)
            {
                string[] dataLines = elem.Value.Split('\n');
                foreach (var dataLine in dataLines)
                {
                    if (dataLine != "")
                    {
                        PowerRecord irradianceRecord = new PowerRecord();
                        irradianceRecord.Kommun = elem.Key;
                        string[] data = dataLine.Split(' ');
                        irradianceRecord.date.year = data[0];
                        irradianceRecord.date.month = data[1];
                        irradianceRecord.date.day = data[2];
                        irradianceRecord.date.hour = data[3];
                        irradianceRecord.Energi = float.Parse(data[4], CultureInfo.InvariantCulture.NumberFormat);
                        irradianceRecords.Add(irradianceRecord);
                    }
                }
            }
            return irradianceRecords;
        }

        static public Dictionary<string, float> GetInstalledPowerDict()
        {
            Dictionary<string, float> dataDict = new Dictionary<string, float>();
            string data = GetInstalledPowerData();
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

            Dictionary<string, float> installedPowerDict = GetInstalledPowerDict();
            List<PowerRecord> irradianceRecords = GetIrradianceRecords();

            foreach (var municipality in installedPowerDict)
            {
                Dictionary<Date, double> powerDict = new Dictionary<Date, double>();
                foreach (var irradiance in irradianceRecords)
                {
                    var area = municipality.Value / (0.15 * 1000); //Kommunal installerad effekt/(verkningsgrad * Irradians vid STC)
                    powerDict[irradiance.date] = irradiance.Energi * 0.15 * 0.9 * area;
                }
                munDict[municipality.Key] = powerDict;
            }
            AddPowerRecord(munDict);
        }

        static public void AddPowerRecord(Dictionary<string, Dictionary<Date, double>> powerDict)
        {
            foreach (var elem in powerDict)
            {
                var powerRecord = new PowerRecord();
                powerRecord.Kommun = elem.Key;
                var lastpowerrecord = powerRecord;

                foreach (var elem2 in elem.Value)
                {
                    powerRecord.date.year = elem2.Key.year;
                    powerRecord.date.month = elem2.Key.month;
                    powerRecord.date.day = elem2.Key.day;
                    powerRecord.Energi += elem2.Value;

                }
                using (var conn = new SqlConnection("Data Source=solkalkdb.chkikmbcmqgq.eu-west-1.rds.amazonaws.com;Initial Catalog=SolkalkDb;Integrated Security=False;User ID=NFK2018;Password=NFKsolkalk;Connect Timeout=30;Encrypt=False;TrustServerCertificate=True;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
                {
                    var query = "INSERT INTO ProducedPower VALUES(@energi,@kommun,@year,@month,@day,@hour)";
                    using (var command = new SqlCommand(query, conn))
                    {
                        command.Parameters.Add("@energi", SqlDbType.Float).Value = powerRecord.Energi;
                        command.Parameters.Add("@kommun", SqlDbType.NChar).Value = powerRecord.Kommun;
                        command.Parameters.Add("@year", SqlDbType.NChar).Value = powerRecord.date.year;
                        command.Parameters.Add("@month", SqlDbType.NChar).Value = powerRecord.date.month;
                        command.Parameters.Add("@day", SqlDbType.NChar).Value = powerRecord.date.day;
                        command.Parameters.Add("@hour", SqlDbType.NChar).Value = "00";
                        command.Connection.Open();
                        command.ExecuteNonQuery();
                        command.Connection.Close();
                    }
                }
            }
        }
    }
}
