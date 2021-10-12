/**
 * Author    Krisjan Oldekamp / Stacktonic.com
 * Email     krisjan@stacktonic.com
 * Article   https://stacktonic.com/article/create-a-user-profile-and-recommender-service-using-big-query-redis-and-gtm-server
 * Based on  https://futurice.com/blog/bigquery-to-memorystore
 */

'use strict';

const Redis = require('ioredis');
const {Storage} = require('@google-cloud/storage');
const split = require('split');

const REDISHOST = process.env.REDISHOST || 'localhost';
const REDISPORT = process.env.REDISPORT || 6379;
const EXPIRATION = process.env.EXPIRATION || 259200;  // Expiration of records in seconds
const FILE_PATH_OUTBOUND = process.env.FILE_PATH_OUTBOUND || 'outbound'; 
const FILE_PATH_PROCESSED = process.env.FILE_PATH_PROCESSED || 'processed'; 
const FILE_PREFIX = process.env.FILE_PREFIX || ''; 

const redisClient = new Redis({
  host: REDISHOST,
  port: REDISPORT,
});

/**
 * Triggered from a change to a Cloud Storage bucket.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.loadCloudStorageToRedis = async(info, context) => {
  
  const path = info.name.split('/')
  const fileName = path[path.length-1]

  if (info.metageneration === '1' && info.name.startsWith(FILE_PATH_OUTBOUND) && fileName.startsWith(FILE_PREFIX)) {

    console.log(`New file upload: gs://${info.bucket}/${info.name}`)

    const storage = new Storage()
    const bucket = storage.bucket(info.bucket);
    const file = bucket.file(info.name);

    let keysWritten = 0;

    try {
      
      // Read file and send to Redis
      file.createReadStream()
        .on('error', error => reject(error))
        .on('response', (response) => {
          // connection to GCS opened
        }).pipe(split())
        .on('data', function (record) {
          if (!record || record === "") return;
          keysWritten++;
          const data = JSON.parse(record);
          redisClient.set(data.key, record, 'EX', EXPIRATION);
        })
        .on('end', () => {
          console.log(`Successfully written ${keysWritten} keys to Memcache Redis.`);

          // Move file to processed folder
          bucket.file(info.name).move(FILE_PATH_PROCESSED + '/' + fileName);
          console.log(`File moved to: ${info.bucket}/${FILE_PATH_PROCESSED}/${fileName}`);
        })
        .on('error', error => reject(error));
    
    } catch(e) {
      console.log(`Error importing ${fileName} to Redis: ${e}`);
    }

  }
};