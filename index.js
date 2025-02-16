const axios = require('axios');
const express = require('express');
require('dotenv').config();

const API_URL = process.env.API_URL;
const DBMAX_API_URL = process.env.DBMAX_API_URL;
const AUTH_KEY = process.env.AUTH_KEY;

const app = express();
app.use(express.json());

let coinCache = { data: [], timestamp: 0 };
let maintainance = false;

async function fetchTopCoins() {
    try {
        const response = await axios.post(API_URL, {
            currency: 'INR',
            sort: 'rank',
            order: 'ascending',
            offset: 0,
            limit: 100,
            meta: true
        }, {
            headers: {
                'content-type': 'application/json',
                'x-api-key': process.env.LIVE_COIN_WATCH_API_KEY
            }
        });
        
        if (response.data && response.data.length) {
            coinCache = {
                data: response.data.map(coin => ({
                    name: coin.name,
                    code: coin.code,
                    rank: coin.rank,
                    rate: coin.rate,
                    volume: coin.volume,
                    cap: coin.cap,
                    circulatingSupply: coin.circulatingSupply,
                    totalSupply: coin.totalSupply,
                    maxSupply: coin.maxSupply,
                    delta_hour: coin.delta?.hour,
                    delta_day: coin.delta?.day,
                    delta_week: coin.delta?.week,
                    website: coin.links?.website,
                    whitepaper: coin.links?.whitepaper
                })),
                timestamp: Date.now()
            };

            if(maintainance) {
                return;
            }

            for (const coin of response.data) {
                await insertCoinHistory(coin);
            }
        }
    } catch (error) {
        console.error('Error fetching top coins:', error.message);
    }
}

async function insertCoinHistory(coin) {
    const historyTable = `${coin.code}_history`;
    const createTableQuery = `CREATE TABLE IF NOT EXISTS ${historyTable} (price REAL, volume REAL, marketCap REAL, timestamp INTEGER);`;
    const insertQuery = `INSERT INTO ${historyTable} (price, volume, marketCap, timestamp) 
                         VALUES (${coin.rate}, ${coin.volume}, ${coin.cap}, ${Date.now() - 10 * 24 * 60 * 60 * 1000});`; // ${Date.now()}
    
    try {
        await axios.post(DBMAX_API_URL, { auth: AUTH_KEY, query: createTableQuery }, { headers: { 'Content-Type': 'application/json' } });
        const response = await axios.post(DBMAX_API_URL, { auth: AUTH_KEY, query: insertQuery }, { headers: { 'Content-Type': 'application/json' } });
        console.log(`Historical data inserted into ${historyTable}:`, response.data);
    } catch (error) {
        console.error(`Error inserting historical data into ${historyTable}:`, error.message);
    }
}

function authenticate(req, res, next) {
    if (req.headers['auth-key'] !== AUTH_KEY) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
}

app.get('/coinsLatest', authenticate, (req, res) => {
    res.json({ data: coinCache.data, timestamp: coinCache.timestamp });
});

app.post('/coinHistory', authenticate, async (req, res) => {
    const { name } = req.body;
    if (!name) return res.status(400).json({ error: 'Coin name is required' });
    
    const historyTable = `${name.toUpperCase()}_history`;
    const query = `SELECT * FROM ${historyTable} ORDER BY timestamp DESC;`;
    
    try {
        const response = await axios.post(DBMAX_API_URL, {
            auth: AUTH_KEY,
            query: query
        }, {
            headers: { 'Content-Type': 'application/json' }
        });
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

async function deleteOldData() { 
    maintainance = true;
    console.log("Maintenance mode ON.");

    if (!coinCache.data.length) {
        console.log("No coins in cache, skipping cleanup.");
        maintainance = false;
        console.log("Maintenance mode OFF.");
        return;
    }

    try {
        for (const coin of coinCache.data) {
            const tableName = `${coin.code}_history`;
            const deleteQuery = `DELETE FROM ${tableName} WHERE timestamp < strftime('%s', 'now', '-7 days') * 1000;`;

            try {
                const response = await axios.post(DBMAX_API_URL, {
                    auth: AUTH_KEY,
                    query: deleteQuery
                }, {
                    headers: { 'Content-Type': 'application/json' }
                });

                console.log(`Deleted old data from ${tableName}:`, response.data);
            } catch (error) {
                console.error(`Error deleting from ${tableName}:`, error.message);
            }
        }
    } finally {
        maintainance = false;
        console.log("Maintenance mode OFF.");
    }
}


// Run every 6 hours
setInterval(deleteOldData, 6 * 60 * 60 * 1000); 
deleteOldData();

setInterval(fetchTopCoins, 20000);

app.listen(5555, () => console.log('Server running on port 5555'));
