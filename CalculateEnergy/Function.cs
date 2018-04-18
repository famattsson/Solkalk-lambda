using System;
using System.Collections.Generic;
using System.Net;
using Amazon.Lambda.Core;
using System.Data.SqlClient;
using System.Text;
using System.IO;
using System.Globalization;



// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]



namespace CalculateEnergy
{

    public class CalculateEnergy
    {


        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>

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
            public string Year;
            public string Month;
            public string Day;
            public string Hour;
            public double Energi;
        }

        public void FunctionHandler(ILambdaContext context)
        {
            CalculateProducedPower();
        }

        static public string GetIrrradianceData()
        {
            var date = DateTime.Today;
            var hour = 0;
            var day = date.AddDays(-1).Day;
            var year = date.Year;
            var month = date.Month;
            var nextDay = date.AddDays(1).Day;
            var requestUrl = String.Format("http://strang.smhi.se/extraction/getseries.php?par=117&m1={0}&d1={1}&y1={2}&h1={3}&m2={4}&d2={5}&y2={6}&h2={7}&lat=58.58&lon=16.15&lev=0",
                month, day, year, hour, month, nextDay, year, hour);

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(requestUrl);
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            Stream responseStream = response.GetResponseStream();
            StreamReader streamReader = new StreamReader(responseStream, Encoding.UTF8);
            string responseBody = streamReader.ReadToEnd();
            return responseBody;
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

        static public Dictionary<Date, float> GetIrradianceDict()
        {
            Dictionary<Date, float> dataDict = new Dictionary<Date, float>();
            string irradianceData = GetIrrradianceData();
            string[] dataLines = irradianceData.Split('\n');

            foreach (var elem in dataLines)
            {
                if (elem != "")
                {
                    string[] data = elem.Split(' ');
                    Date dataDate = new Date();
                    dataDate.date = elem;
                    dataDate.year = data[0];
                    dataDate.month = data[1];
                    dataDate.day = data[2];
                    dataDate.hour = data[3];
                    dataDict[dataDate] = float.Parse(data[4], CultureInfo.InvariantCulture.NumberFormat);
                }
            }
            return dataDict;
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
            Dictionary<Date, float> irradianceDict = GetIrradianceDict();

            foreach (var municipality in installedPowerDict)
            {
                Dictionary<Date, double> powerDict = new Dictionary<Date, double>();
                foreach (var irradiance in irradianceDict)
                {
                    var area = municipality.Value / (0.15 * 1000); //Kommunal installerad effekt/(verkningsgrad * Irradians vid STC)
                    powerDict[irradiance.Key] = irradiance.Value * 3600 * 0.15 * 0.9 * area;
                }
                munDict[municipality.Key] = powerDict;
            }
            AddPowerRecord(munDict);
        }

        static public void AddPowerRecord(Dictionary<string, Dictionary<Date, double>> powerDict)
        {
            foreach (var elem in powerDict)
            {
                foreach (var elem2 in elem.Value)
                {
                    var powerRecord = new PowerRecord();
                    powerRecord.Kommun = elem.Key;
                    powerRecord.Year = elem2.Key.year;
                    powerRecord.Month = elem2.Key.month;
                    powerRecord.Day = elem2.Key.day;
                    powerRecord.Hour = elem2.Key.hour;
                    powerRecord.Energi = elem2.Value;
                    using (var conn = new SqlConnection("Data Source=solkalkdb.chkikmbcmqgq.eu-west-1.rds.amazonaws.com;Initial Catalog=SolkalkDb;Integrated Security=False;User ID=NFK2018;Password=NFKsolkalk;Connect Timeout=30;Encrypt=False;TrustServerCertificate=True;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
                    {
                        var query = "INSERT INTO ProducedPower VALUES(@energi,@kommun,@year,@month,@day,@hour)";
                        using (var command = new SqlCommand(query, conn))
                        {
                            command.Connection.Open();
                            command.ExecuteNonQuery();
                            command.Connection.Close();
                        }
                    }
                }
            }
        }
    }
}
