using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json.Linq;

namespace BitGradingFuncation
{
    public static class Function1
    {

        private static string str = GetConnectionString();

        [FunctionName("Function1")]
        public static void Run([TimerTrigger("0 */1 * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");
            ProcessImage();


        }

        private static string GetConnectionString()
        {
            return "Server=tcp:bitgrading.database.windows.net,1433;Initial Catalog=BitGrading;Persist Security Info=False;User ID=bdg;Password=hackathon@2018;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
        }

        private static void InsertDummyData()
        {
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                FileInfo finfo = new FileInfo(@"C:\Image\8.750-U713M-U03115-38285-2017-12-01-R112404_B1.JPG");
                byte[] btImage = new byte[finfo.Length];
                FileStream fStream = finfo.OpenRead();
                fStream.Read(btImage, 0, btImage.Length);
                fStream.Close();
                using (SqlCommand sqlCommand = new SqlCommand(
      "INSERT INTO [bitImagestable] (WellID,SerialNumber, " +
      "BitImage) VALUES(@wellId, @serailNumber, @bitImage)",
      conn))
                {
                    sqlCommand.Parameters.AddWithValue("@wellId", "ABC123");
                    sqlCommand.Parameters.AddWithValue("@serailNumber", "XYZ123");
                    SqlParameter imageParameter =
                                   new SqlParameter("@bitImage", SqlDbType.Image);
                    imageParameter.Value = btImage;

                    sqlCommand.Parameters.Add(imageParameter);

                    sqlCommand.ExecuteNonQuery();
                    conn.Close();
                }
            }

        }

        private static void ProcessImage()
        {
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand("select ImageId,BitImage,WellId from [dbo].[bitImagestable] where IsProcessed = 0", conn))
                {
                    SqlDataAdapter adpt = new SqlDataAdapter(cmd);
                    DataSet dataSet = new DataSet();
                    adpt.Fill(dataSet, "Image");
                    DataTable dt = dataSet.Tables["Image"];
                    foreach (DataRow row in dt.Rows)
                    {
                        int ImageId = (int)row[0];
                        byte[] byteData = (byte[])row[1];
                        string wellId = (string)row[2];
                        ProcessPredictions(byteData, wellId).Wait();
                        UpdateImageId(ImageId);
                    }
                    
                }
            }
        }

        private static void UpdateImageId(int imageId)
        {
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                using (SqlCommand sqlCommand = new SqlCommand("Update [bitImagestable] Set IsProcessed = 1 Where ImageId =" + imageId, conn))
                {
                    sqlCommand.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        static async Task ProcessPredictions(byte[] byteData,string wellId)
        {
            using (var content = new ByteArrayContent(byteData))
            {
                var client = new HttpClient();
                HttpResponseMessage response;
                // Request headers - replace this example key with your valid subscription key.
                //client.DefaultRequestHeaders.Add("Prediction-Key", "3b76b70758d6447db468b647bb7c3121");
                client.DefaultRequestHeaders.Add("Prediction-Key", "3b76b70758d6447db468b647bb7c3121");

                // Prediction URL - replace this example URL with your valid prediction URL.
                //string url = "https://southcentralus.api.cognitive.microsoft.com/customvision/v2.0/Prediction/f6d33f56-7f78-46f7-86c3-47b88320e6b3/image?iterationId=e9ab5d4c-d902-4b02-aef6-3e27f6c5ae06";
                string url = "https://southcentralus.api.cognitive.microsoft.com/customvision/v2.0/Prediction/b7c376c7-1743-4403-92c7-af5c6cdc697c/image?iterationId=28cb3687-9018-47af-a9df-f5ff3ddc106e";
                content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                response = await client.PostAsync(url, content);
                string res = (await response.Content.ReadAsStringAsync());

                var jo = Newtonsoft.Json.Linq.JObject.Parse(res);
                InsertPredicationData(jo,wellId);


            }
        }

        private static void InsertPredicationData(JObject jo,string wellId)
        {
            try
            {
                for (int i = 0; i < 25; i++)
                {
                    var _BT = jo["predictions"][i];
                    var tagName = _BT["tagName"].ToString();
                    var probability = _BT["probability"].ToString();

                    using (SqlConnection conn = new SqlConnection(str))
                    {
                        conn.Open();
                        using (SqlCommand sqlCommand = new SqlCommand(
              "INSERT INTO [BitDullGrading] (WellID,TagName, " +
              "Probability) VALUES(@wellId, @tagName, @probability)",
              conn))
                        {
                            sqlCommand.Parameters.AddWithValue("@wellId", wellId);
                            sqlCommand.Parameters.AddWithValue("@tagName", tagName);
                            sqlCommand.Parameters.AddWithValue("@probability", probability);
                            sqlCommand.ExecuteNonQuery();
                            conn.Close();
                        }
                    }

                }
            }
            catch(Exception ex)
            {

            }
        }
    }
}
