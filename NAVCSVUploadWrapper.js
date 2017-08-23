// dependencies
const readline = require('readline');
const aws = require('aws-sdk');
aws.config.update({ region: 'eu-west-1' });
var DV = require('./dateHelper.js');
var RE = require('./raiseError.js');
var error = false;
var errorMessage = [];
var functionName;

exports.handler = (event, context, callback) => {
    // read S3 object stream
    console.log("INFO: starting upload wrapper");
    callback = callback;
    var s3 = new aws.S3({ apiVersion: '2006-03-01' });
    var bucket = event.Records[0].s3.bucket.name;
    var key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    var params = {
        Bucket: bucket,
        Key: key
    };
    functionName = context.functionName;
    console.log("INFO: bucket ID- " + bucket + ", key- " + key);
    if(key.substr((key.length -3), 3) != 'csv'){
        errorMessage.push("ERROR: invalid file format - csv required to process upload");
        var errorMessageX = errorMessage;
        errorMessage = [];
        error =false;
        RE.raiseError(generateUUID(), 'unknown', functionName, errorMessageX, callback);
    }else{
        mainProcess(s3, params, context, event, processFile, callback);
    }
}


//get the header object
mainProcess = function (s3, params, context, event, _callback, callback) {
    console.log("INFO: begin main");
    var uploadObj = {
        uploadUUID: generateUUID(),
        user: "",
        organisation: "",
        comments: "",
        frequency: "",
        category: "",
        commences: "",
        sequence: 0
    }
    //waitfor s3 object

    //get request to S3 for header object
    s3.waitFor('objectExists', params, function (err, response) {
        if (err) {
            //context.callbackWaitsForEmptyEventLoop = false;
            console.log("ERROR: failed to get header from S3", err);
            console.log("ERROR: No NAV values have been updated");
            errorMessage.push("No NAV values updates as failed to retrieve S3 header object");
            var errorMessageX = errorMessage;
            errorMessage = [];
            error =false;
            RE.raiseError(uploadObj.uploadUUID, uploadObj.user, functionName, errorMessageX, callback);
        } else {
            console.log("SUCCESS: got header from S3");
            uploadObj.user = response.Metadata.user;
            if (uploadObj.user == undefined) {
                uploadObj.user = "NULL";
                error = true;
                errorMessage.push("No NAV values updated as User must not be blank");
            }
            uploadObj.organisation = response.Metadata.organisation;
            if (uploadObj.organisation == undefined) {
                uploadObj.organisation = "NULL";
            }
            uploadObj.comments = response.Metadata.comments;
            if (uploadObj.comments == undefined) {
                uploadObj.comments = "NULL";
            }
            uploadObj.frequency = response.Metadata.frequency;
            if (uploadObj.frequency == undefined) {
                uploadObj.frequency = "NULL";
                error = true;
                errorMessage.push("No NAV values updated as Frequency must not be blank");
            }
            uploadObj.category = response.Metadata.category;
            if (uploadObj.category == undefined) {
                uploadObj.category = "NULL";
                error = true;
                errorMessage.push("No NAV values updated as Category must not be blank");
            }
            uploadObj.commences = response.Metadata.commences;
            if (uploadObj.commences == undefined) {
                uploadObj.commences = "NULL";
                error = true;
                errorMessage.push("No NAV values updated as Period Commences must not be blank");
            }
            uploadObj.sequence = createSequence(uploadObj.commences, uploadObj.frequency);
            if (error) {
                console.log("ERROR: Invalid upload parameters.");
                console.log("ERROR: No NAV values have been updated");
                RE.raiseError(uploadObj.uploadUUID, uploadObj.user, callback);
            } else {
                console.log("SUCCESS: retrieved S3 head object with valid parameters");
                _callback(uploadObj, params, s3, event, context, callback);
            }
        }
    });
}

processFile = function (uploadObj, params, s3, event, context, callback) {
    var ISIN;
    var NAV;
    var calculationDate;
    var header = true;
    var calculateSRRI;
    var count = 0;
    //get the document 
    const rl = readline.createInterface({
        input: s3.getObject(params).createReadStream()
    });
    //first get the number of rows...scale out dynamoDB

    /* TODO validate; if error publish unprocessed file topic and quit */
    console.log("INFO: begin processing file");
    rl.on('line', function (line) {
        if (header == false) {
            var array = line.split(",");
            ISIN = array[0];
            NAV = array[1];
            calculationDate = array[2];
            calculateSRRI = array[3];
            count += 1;

            //send the share class, for processing
            var message = {
                requestUUID: uploadObj.uploadUUID,
                ISIN: ISIN,
                NAV: NAV,
                category: uploadObj.category,
                frequency: uploadObj.frequency,
                user: uploadObj.user,
                sequence: uploadObj.sequence.toString(),
                calculateSRRI: calculateSRRI,
                calculationDate: calculationDate,
                stateMachine: "processNAVUpload"
            }

            console.log("INFO: request update NAV for ISIN :" + ISIN);
            sendLambdaSNS(event, context, message, "arn:aws:sns:eu-west-1:437622887029:NAVUploaded", "update NAV request");
        } else {
            header = false;
        }
    }).on('close', function () {
        console.log("INFO: finished processing all records");
        //write header to db
        writeDynamoRecs(uploadObj.uploadUUID, uploadObj.user, uploadObj.organisation, uploadObj.frequency, uploadObj.category, count, uploadObj.comments, uploadObj.sequence, callback)
        //publish completion to SNS
        var success = {
            Result: "Successfully process all records"
        };
        error = false;
        errorMessage = [];
        callback(null, success);
 });
}

writeDynamoRecs = function (uuid, user, organisation, frequency, category, count, comments, sequence, callback) {
    //write to the database
    console.log("INFO: write record to Upload History");
    var dynamo = new aws.DynamoDB();
    var tableName = "UploadHistory";
    var item = {
        RequestUUID: { "S": uuid },
        CreatedTimeStamp: { "N": new Date().getTime().toString() },
        CreatedDateTime: { "S": new Date().toUTCString() },
        Organisation: { "S": organisation },
        CreateUser: { "S": user },
        Frequency: { "S": frequency },
        Category: { "S": category },
        ShareClassCount: { "N": count.toString() },
        Sequence: { "N": sequence.toString() },
        Comments: { "S": comments }
    }
    console.log(item);
    var params = {
        TableName: tableName,
        Item: item
    }

    dynamo.putItem(params, function (err, data) {
        if (err) {
            error=true;
            errorMessage.push("NAV update requests were submitted successfully but update to Upload History failed.")
            console.log("ERROR: failed to update the Upload History table, NAV updates may have occurred - contact administrator", err);
            callback(err);
     }else {
            console.log("SUCCESS: the Upload History table has been updated", data);
        }
    });
}

sendLambdaSNS = function (event, context, message, topic, subject) {
    var sns = new aws.SNS();
    console.log("send the ", message);
    var params = {
        Message: JSON.stringify(message),
        Subject: subject,
        TopicArn: topic
    };
    sns.publish(params, context.done);
    return null;
}

function generateUUID() { // Public Domain/MIT
    var d = new Date().getTime();
    if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
        d += performance.now(); //use high-precision timer if available
    }
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        var r = (d + Math.random() * 16) % 16 | 0;
        d = Math.floor(d / 16);
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
}

createSequence = function (dateIn, frequency) {
    var dateInDate = DV.dateFactory(dateIn);

    if (DV.isValidDate(dateInDate)) {
        var sequence = DV.sequenceFactory(dateInDate, frequency);
    } else {
        sequence = "";
        error = true;
        errorMessage.push("No NAV values updated as Period Commencement date is invalid");
    }
    return sequence;
}



   