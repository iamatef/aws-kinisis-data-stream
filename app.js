const AWS = require('aws-sdk');
const kinesis = new AWS.Kinesis({ region: 'us-east-1' }); // Replace 'your-region' with your AWS region

const streamName = 'my-demo-kinesis-data-stream';

const putDummyDataToStream = async () => {
  try {
    for (let i = 0; i < 10; i++) { // Sending 10 dummy records
      const recordData = `Record ${i}`;
      const params = {
        Data: JSON.stringify({ data: recordData }),
        PartitionKey: `partitionKey-${i}`,
        StreamName: streamName,
      };

      var putRecord = await kinesis.putRecord(params).promise();
      console.log(putRecord);
      console.log(`Successfully added record: ${recordData}`);
    }
  } catch (error) {
    console.error('Error adding records to the stream:', error);
  }
};

 
const getRecordsFromStream = async () => {
    
    //getting the shard iterator
    const params = {
      ShardIteratorType: 'TRIM_HORIZON', // Start reading from the beginning of the stream
      StreamName: streamName,
      ShardId: 'shardId-000000000000', // Replace 'your-shard-id' with your shard ID
    };
  
    try {
      var shardIterator = (await kinesis.getShardIterator(params).promise()).ShardIterator;

      //log the shard iterator
      console.log('shardIterator', shardIterator);
  
      const processRecords = async (shardIterator) => {
       
        //loop till records count is greater than 0
        var recordsCount = 0;
        var  recordsResponse;

        //while records count is 0, keep looping
        
        //loop 10 times
        for (let i = 0; i < 10; i++) {
            recordsResponse = await kinesis.getRecords({ ShardIterator: shardIterator }).promise();
            
            //set new shard iterator
            shardIterator = recordsResponse.NextShardIterator;
            
            recordsCount = recordsResponse.Records.length;
            console.log('recordsCount', recordsCount);

            if(recordsCount > 0){
                break;
            }

        }
 
        //log recordsResponse
        console.log('recordsResponse', recordsResponse);
        
        const records = recordsResponse.Records;
  
        for (const record of records) {
          const payload = JSON.parse(record.Data.toString());
          console.log('Received record:', payload);
        }
  
        if (records.length > 0) {
          await processRecords(recordsResponse.NextShardIterator);
        }
      };
  
      await processRecords(shardIterator);
    } catch (error) {
      console.error('Error reading records from the stream:', error);
    }
  };
  
  
   

//putDummyDataToStream();


getRecordsFromStream();

