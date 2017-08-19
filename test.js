// Our Lambda function fle is required 
var index = require('./NAVCSVUploadWrapper.js');

// The Lambda context "done" function is called when complete with/without error
var context = {
    done: function (err, result) {
        console.log('------------');
        console.log('Context done');
        console.log('   error:', err);
        console.log('   result:', result);
    }
};

// Simulated S3 bucket event
var event = {
    Records: [
        {
            s3: {
                bucket: {
                    name: 'regstonedemo'
                },
                object: {
                    key: 'Test.csv'
                }
            }
        }
    ]
};

// Call the Lambda function
index.handler(event, context);