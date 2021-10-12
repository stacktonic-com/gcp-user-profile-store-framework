/**
 * Author    Krisjan Oldekamp / Stacktonic.com
 * Email     krisjan@stacktonic.com
 * Article   https://stacktonic.com/article/create-a-user-profile-and-recommender-service-using-big-query-redis-and-gtm-server
 */

'use strict'

const Redis = require('ioredis');

const REDISHOST = process.env.REDISHOST || 'localhost';
const REDISPORT = process.env.REDISPORT || 6379;
const QUERY_REDIS_KEY = process.env.QUERY_REDIS_KEY || 'id';
const QUERY_SECRET_KEY = process.env.QUERY_SECRET_KEY || 'secret';
const SECRET = process.env.SECRET || '';

const redisClient = new Redis({
  host: REDISHOST,
  port: REDISPORT,
});

/**
 * HTTP function 
 * 
 * @param {Object} req Cloud Function request context.
 * @param {Object} res Cloud Function response context.
 */
exports.httpServeFromRedis = (req, res) => {

    res.set('Access-Control-Allow-Origin', "*");
    res.set('Access-Control-Allow-Methods', 'GET');

    if (SECRET == '' || (req.query[QUERY_SECRET_KEY] == SECRET)) {
        if (typeof req.query[QUERY_REDIS_KEY] != 'undefined') {
            const key = req.query[QUERY_REDIS_KEY];
            
            redisClient.get(key, function (err, result) {
                if (err) {
                    console.error(err);
                    res.status(400).json({ error: 'Error' });
                } else {
                    if (result !== null) {
                        const data = JSON.parse(result);
                        res.status(200).json(data);
                    } else {
                        res.status(400).json({ error: 'Key not found' });
                    }
                }
            });
        } else {
            res.status(400).json( { error: 'No key specified' } );
        }
    } else {
        res.status(401).json( { error: 'Unauthorized' } );
    }
};