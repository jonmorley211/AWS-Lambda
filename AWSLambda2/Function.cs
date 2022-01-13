using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using static Amazon.S3.Util.S3EventNotification;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AWSLambda2
{
    public class Function
    {
        
        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public string FunctionHandler(S3Event inEvent, ILambdaContext context)
        {

            string message = DateTime.Now.ToLongTimeString() + "_test_message";

            AmazonSimpleNotificationServiceClient SNSClient = new AmazonSimpleNotificationServiceClient();

            List<S3EventNotificationRecord> recordsList = inEvent.Records;

            foreach(S3EventNotificationRecord record in recordsList)
            {
                message += "|_objectName=" + record.S3.Object.Key;
            }

            PublishRequest pubReq = new PublishRequest
                (
                    System.Environment.GetEnvironmentVariable("sns_topic_arn"),
                    message
                );

            pubReq.MessageDeduplicationId = "123456";
            pubReq.MessageGroupId = "S3-bucket-input-group";

            Task<PublishResponse> publish = SNSClient.PublishAsync(pubReq);

            publish.Wait();

            return message;
        }
    }
}
