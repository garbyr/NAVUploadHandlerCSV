const aws = require('aws-sdk');
aws.config.update({ region: 'eu-west-1' });

raiseError = function(requestUUID, user, functionName, errorMessage, callback){
    var errorObj = {
        requestUUID: requestUUID,
        user: user,
        function: functionName,
        messages: errorMessage,
    }
//write the error to dynamo directly from this wrapper
    var dynamo = new aws.DynamoDB();
    var tableName = "ErrorLog";
    var item = {
        RequestUUID: { "S": requestUUID },
        CreatedTimeStamp: { "N": new Date().getTime().toString() },
        CreatedDateTime: { "S": new Date().toUTCString() },
        CreateUser: { "S": user },
        Function: { "S": functionName},
        Errors: { "S": JSON.stringify(errorMessage) }
    }
    //reset global variables just in case container is reused
    var params = {
        TableName: tableName,
        Item: item
    }
    console.log(params);
    dynamo.putItem(params, function (err, data) {
        if (err) {
            console.log("ERROR: error table not updated", err);
             callback(errorObj);
        }
        else {
            console.log("SUCCESS: error table updated", data);
             callback(errorObj);
        }

    });
}

exports.raiseError = raiseError;

   